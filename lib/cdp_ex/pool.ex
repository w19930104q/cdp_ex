defmodule CDPEx.Pool do
  @moduledoc """
  A fixed-size pool of reusable `CDPEx.Browser` processes, with isolated,
  self-healing **leases**.

  Launching Chrome is expensive — a cold start can take several seconds on a
  constrained host, with a fresh profile each time. A pool keeps browsers warm
  and hands them out for reuse, so a per-job fetch no longer pays a launch on
  every call.

      {:ok, pool} = CDPEx.Pool.start_link(size: 2, launch_opts: [headless: true])

      CDPEx.Pool.with_page(pool, fn page ->
        {:ok, _} = CDPEx.Page.navigate(page, "https://example.com")
        CDPEx.Page.html(page)
      end)

  ## Leases

  Every `checkout/2` starts a `CDPEx.Lease` — a small process that identifies
  that one rental and owns its fate. `checkout/2` returns the **lease pid**,
  not the browser pid; resolve the browser with `CDPEx.Lease.browses/1` (or use
  `with_browser/3` / `with_page/3`, which do it for you) and end the rental with
  `checkin/2`. The lease makes each rental independent:

    * **Timeout, cancellation and owner death only clean up its own resources.**
      A caller that crashes while holding a lease settles just that lease; other
      rentals and other waiters are untouched.
    * **A late page can't bleed into the next rental.** A `new_page/2` that
      times out may still open its tab afterwards, unowned. `with_page/3` marks
      that lease's browser **contaminated** (`CDPEx.Lease.contaminate/2`), so at
      check-in the browser is retired and replaced instead of going back on the
      shelf with a stray tab — the next lease always gets a clean browser.
    * **Suspicious browsers retire and self-heal.** An owner that dies mid-rental
      also retires the browser it was using (its work may be half-done), and a
      browser that crashes is dropped outright. Each retirement frees capacity;
      the next waiting checkout launches a replacement, so `:size` heals itself
      and no waiter is ever handed a dead or retired instance.

  Browsers are launched **lazily** up to `:size`. `checkout/2` blocks (up to
  `:checkout_timeout`) when every browser is busy. Put the pool under your
  supervision tree; its `terminate/2` stops every browser, reaping Chrome.

  Browser launches are **asynchronous** — each cold start (a cold Chrome can take a
  few seconds) runs in its own task, so the pool keeps serving checkouts, checkins,
  and timeouts while browsers warm up, and a pool growing to `:size` under load
  launches them concurrently. `:checkout_timeout` is honored even during warmup.

  ## Telemetry

  `[:cdp_ex, :pool, :browser_retired]` fires whenever a browser is taken out of
  rotation rather than returned cleanly — measurements `%{system_time: ...}`,
  metadata `%{reason: reason}` where `reason` is one of:

    * `{:contaminated, inner}` — a checked-in browser was marked suspect (the
      typical case: a timed-out `new_page/2` whose tab may still open late)
    * `{:abandoned, exit_reason}` — its owner died while holding the lease
    * `{:lease_stopped, reason}` — a lease stopped any other way, so its browser
      is treated as untrusted

  A browser that crashed outright emits the usual `[:cdp_ex, :error]` fault
  event(s) instead; its lease frees the slot and the pool backfills it without
  a retirement event.

  ## Options

    * `:size` — maximum number of browsers (default `1`)
    * `:launch_opts` — options passed to each `CDPEx.Browser` (see `CDPEx.Chrome`); any
      `:owner` here is ignored — the pool owns its browsers and sets it itself
    * `:checkout_timeout` — ms to wait for a free browser (default `5_000`)
    * `:name` — registers the pool process
  """

  use GenServer

  alias CDPEx.Browser
  alias CDPEx.Exits
  alias CDPEx.Lease
  alias CDPEx.ProcessLabel
  alias CDPEx.Telemetry

  require Logger

  @default_size 1
  @default_checkout_timeout 5_000
  # Per-launch ceiling for terminate/2's drain of in-flight launches (see terminate/2).
  @launch_drain_timeout 5_000

  defstruct [
    :size,
    :launch_opts,
    :start_fun,
    :task_sup,
    available: [],
    # lease_pid => {browser_pid, owner_pid}. One entry per live rental; the lease
    # pid is the rental's identity, so every settle path touches only its own slot.
    busy: %{},
    waiting: :queue.new(),
    # task_ref => true for each in-flight async launch; counts toward :size so
    # count + map_size(launching) never exceeds it.
    launching: %{},
    count: 0
  ]

  @type t :: %__MODULE__{}

  # ── Public API ──────────────────────────────────────────────────────────────

  @doc "Starts a pool. See the moduledoc for options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {gen_opts, pool_opts} = Keyword.split(opts, [:name])

    if !Keyword.has_key?(pool_opts, :start_fun),
      do: Browser.validate_launch_opts!(Keyword.get(pool_opts, :launch_opts, []))

    GenServer.start_link(__MODULE__, pool_opts, gen_opts)
  end

  @doc false
  def child_spec(opts) do
    # Derive a distinct id from :id/:name so several pools can run under one
    # supervisor without colliding on the default __MODULE__ id. terminate/2 stops
    # every browser sequentially (each takes a few seconds to reap Chrome), so give
    # the supervisor headroom over the GenServer 5s default.
    id = Keyword.get(opts, :id, Keyword.get(opts, :name, __MODULE__))
    %{id: id, start: {__MODULE__, :start_link, [opts]}, shutdown: 30_000}
  end

  @doc """
  Borrows a browser under a fresh lease, blocking up to `timeout` ms when all are
  busy.

  Returns `{:ok, lease}` where `lease` is a `CDPEx.Lease` pid — resolve the
  browser with `CDPEx.Lease.browses/1` and **always** finish with `checkin/2`
  (or use `with_browser/3` / `with_page/3`, which do both for you). Other
  returns: `{:error, :timeout}` if no browser freed in time, `{:error, reason}`
  if a browser had to be launched and failed — a launch error is reported to a
  waiting caller and is generally worth retrying. A caller still waiting when
  the pool stops gets `{:error, :noproc}`.
  """
  @spec checkout(GenServer.server(), timeout()) :: {:ok, pid()} | {:error, term()}
  def checkout(pool, timeout \\ @default_checkout_timeout) do
    # Wait indefinitely on the outer call; the pool's own per-request timer is the
    # authoritative timeout (it removes the waiter and replies {:error, :timeout}).
    # This avoids a stale waiter when a slow launch blocks the pool past a tight
    # outer deadline. A pool that stops mid-wait surfaces as {:error, :noproc}.
    GenServer.call(pool, {:checkout, timeout}, :infinity)
  catch
    :exit, {:noproc, _} -> {:error, :noproc}
    :exit, {:normal, _} -> {:error, :noproc}
    :exit, {:shutdown, _} -> {:error, :noproc}
    :exit, {{:shutdown, _}, _} -> {:error, :noproc}
    # A pool that crashed mid-wait is just as gone.
    :exit, {reason, {GenServer, :call, _}} when reason != :calling_self -> {:error, :noproc}
  end

  @doc """
  Ends a lease started with `checkout/2`. A clean lease returns its browser to
  the pool; a contaminated or ownerless lease retires it instead. Idempotent —
  a second call (or one racing the lease's own death) is a no-op.
  """
  @spec checkin(GenServer.server(), pid()) :: :ok
  def checkin(_pool, lease) when is_pid(lease), do: Lease.done(lease)

  @doc """
  Runs `fun` with a checked-out browser pid, ending the lease afterwards (even if
  `fun` raises). Returns `fun`'s value, or `{:error, reason}` if no browser was
  free or the lease ended before it could resolve the browser.
  """
  @spec with_browser(GenServer.server(), (pid() -> result), timeout()) ::
          result | {:error, term()}
        when result: var
  def with_browser(pool, fun, timeout \\ @default_checkout_timeout) when is_function(fun, 1) do
    case checkout(pool, timeout) do
      {:ok, lease} ->
        try do
          with {:ok, browser} <- Lease.browses(lease) do
            fun.(browser)
          end
        after
          checkin(pool, lease)
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Runs `fun` with a fresh page on a leased browser, cleaning up the page and
  ending the lease afterwards. The pooled counterpart of `CDPEx.with_page/3` —
  it reuses a warm browser instead of launching one per call.

  `opts` are forwarded to `CDPEx.with_page/3` (e.g. `:prevent_alerts`, or
  `:deadline`, which closes the page — not the pooled browser — and whose clock
  starts after checkout, so it doesn't cover the wait for a free browser); pass
  `:checkout_timeout` to bound that wait.
  Returns `fun`'s value, or `{:error, reason}`.

  If `new_page/2` times out (`{:error, {:timeout, :new_page}}`) the browser may
  still open that page later with no owner, so the lease is **contaminated**: its
  browser is retired on check-in and a replacement is launched, rather than
  handed to the next job with a stray tab. A `:deadline` firing after the page
  exists is different — the deadline watchdog closes that specific page, so the
  browser stays clean and returns to rotation.
  """
  @spec with_page(GenServer.server(), (CDPEx.Page.t() -> result), keyword()) ::
          result | {:error, term()}
        when result: var
  def with_page(pool, fun, opts \\ []) when is_function(fun, 1) do
    {timeout, page_opts} = Keyword.pop(opts, :checkout_timeout, @default_checkout_timeout)

    case checkout(pool, timeout) do
      {:ok, lease} ->
        try do
          with {:ok, browser} <- Lease.browses(lease) do
            run_leased_page(pool, lease, browser, fun, page_opts)
          end
        after
          checkin(pool, lease)
        end

      {:error, _} = error ->
        error
    end
  end

  # Open the page through the ordinary CDPEx.with_page machinery (deadline, page
  # cleanup and exit containment all live there). The pool only adds lease
  # hygiene: a timed-out new_page can still complete late and unowned, so retire
  # that browser rather than risk the late tab — e.g. a previous job's login page
  # — surfacing in the next rental.
  defp run_leased_page(_pool, lease, browser, fun, page_opts) do
    result = CDPEx.with_page(browser, fun, page_opts)

    if match?({:error, {:timeout, :new_page}}, result) do
      Lease.contaminate(lease, {:timeout, :new_page})
    end

    result
  end

  @doc "Stops the pool, stopping every browser (and reaping Chrome). `:ok` if it is already gone."
  @spec stop(GenServer.server()) :: :ok
  def stop(pool) do
    GenServer.stop(pool, :normal)
  catch
    :exit, {reason, {GenServer, :stop, _}} when reason != :calling_self ->
      Exits.warn_unclean_stop("CDPEx.Pool", reason)
  end

  # ── GenServer callbacks ─────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    # Trap exits: browsers and leases are start_linked, so their crashes arrive as
    # {:EXIT, _, _} messages we settle (retire/drop/relaunch) rather than taking the
    # pool down, and terminate/2 runs to reap Chrome.
    Process.flag(:trap_exit, true)

    size = Keyword.get(opts, :size, @default_size)
    ProcessLabel.set({:cdp_pool, size})

    # Owns the async browser-launch tasks (see start_one_launch/1). Linked to the pool, so
    # it — and any in-flight launches — go down with us.
    {:ok, task_sup} = Task.Supervisor.start_link()

    {:ok,
     %__MODULE__{
       size: size,
       launch_opts: Keyword.get(opts, :launch_opts, []),
       start_fun: Keyword.get(opts, :start_fun, &Browser.start_link/1),
       task_sup: task_sup
     }}
  end

  @impl true
  def handle_call({:checkout, timeout}, from, state) do
    # Enqueue then dispatch: a request is served at once if there's capacity,
    # otherwise it waits in the queue for a checkin/crash to free a browser.
    timer = arm_timeout(from, timeout)
    {:noreply, dispatch(%{state | waiting: :queue.in({from, timer}, state.waiting)})}
  end

  @impl true
  def handle_info({:checkout_timeout, from}, state) do
    case remove_waiter(state.waiting, from) do
      {:ok, _timer, waiting} ->
        GenServer.reply(from, {:error, :timeout})
        {:noreply, %{state | waiting: waiting}}

      :error ->
        # Served between the timer firing and now — nothing to do.
        {:noreply, state}
    end
  end

  def handle_info({ref, result}, %__MODULE__{launching: launching} = state)
      when is_reference(ref) and is_map_key(launching, ref) do
    # An async launch task returned (a started browser, or a start error). Stop monitoring
    # it — the task's own :DOWN follows, so flush it — then act on the result.
    Process.demonitor(ref, [:flush])
    state = %{state | launching: Map.delete(launching, ref)}

    case result do
      {:ok, browser} -> {:noreply, browser_launched(state, browser)}
      {:error, reason} -> {:noreply, launch_failed(state, reason)}
      # A start_fun that returns something other than {:ok, _} / {:error, _} (e.g. :ignore,
      # valid per GenServer.on_start) is treated as a failed launch rather than crashing us.
      other -> {:noreply, launch_failed(state, {:unexpected_launch_result, other})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %__MODULE__{launching: launching} = state)
      when is_map_key(launching, ref) do
    # A launch task crashed before returning a result (it never sent {ref, result}).
    {:noreply, launch_failed(%{state | launching: Map.delete(launching, ref)}, reason)}
  end

  def handle_info({:EXIT, lease, reason}, state) when is_map_key(state.busy, lease) do
    # A lease settled. The lease is linked (not just monitored), so its stop reason
    # arrives here as the exit reason — unwrap the lease's end_reason (see
    # CDPEx.Lease). Settle only this lease's slot; every other rental is untouched.
    {:noreply, state |> settle_lease(lease, lease_reason(reason)) |> dispatch()}
  end

  def handle_info({:EXIT, task_sup, reason}, %__MODULE__{task_sup: task_sup} = state) do
    # Our launch-task supervisor died — without it we can't launch browsers (a checkout
    # would crash on async_nolink to a dead sup). Stop with an attributable reason so our
    # own supervisor restarts us with a fresh task_sup, rather than limping on as a zombie.
    {:stop, {:task_sup_down, reason}, state}
  end

  def handle_info({:EXIT, browser, _reason}, state) do
    # A pooled browser stopped/crashed. It may be available, busy (its lease will
    # settle separately with :browser_gone), or already removed by that lease.
    # Drop it wherever it is; capacity frees up so dispatch can launch a replacement.
    {:noreply, state |> drop_browser(browser) |> dispatch()}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # Reply to anyone still blocked in checkout/2 first, so they get a clean
    # {:error, :noproc} without waiting on the (possibly multi-second) serial
    # browser teardown below.
    Enum.each(:queue.to_list(state.waiting), fn {from, timer} ->
      _ = cancel_timer(timer)
      GenServer.reply(from, {:error, :noproc})
    end)

    # Leases are linked to us; stopping the browsers below makes their monitors
    # fire, but nothing will read those messages again.
    Enum.each(state.available, &safe_stop/1)
    Enum.each(Map.values(state.busy), fn {browser, _owner} -> safe_stop(browser) end)

    # An in-flight launch may finish a browser we never adopted: once the task runs
    # Process.unlink/1, that browser is linked to nobody, so killing task_sup won't reap it.
    # task_sup is still alive here, so each launch can still deliver — wait briefly per ref to
    # catch a just-completed one and stop it (or its terminal :DOWN/error), so a shutdown
    # mid-launch doesn't orphan Chrome. One SHARED deadline caps the whole drain at
    # @launch_drain_timeout regardless of :size, so it can't blow past child_spec's
    # shutdown: 30_000 (which would let the supervisor :brutal_kill us mid-drain).
    drain_deadline = System.monotonic_time(:millisecond) + @launch_drain_timeout

    Enum.each(Map.keys(state.launching), fn ref ->
      remaining = max(drain_deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {^ref, {:ok, browser}} -> safe_stop(browser)
        {^ref, _other} -> :ok
        {:DOWN, ^ref, :process, _pid, _reason} -> :ok
      after
        remaining ->
          # Budget spent before this launch delivered — its browser (if it completed) may
          # leak. Surface it rather than leaking silently. (remaining == 0 once the shared
          # deadline is used up, so later refs are skipped without a spurious wait.)
          if remaining > 0 do
            Logger.warning(
              "[CDPEx.Pool] a browser launch didn't complete within the shutdown drain; its Chrome may be orphaned"
            )
          end
      end
    end)

    :ok
  end

  # ── pool mechanics ──────────────────────────────────────────────────────────

  # Serve queued waiters: hand out idle browsers, then start launches (concurrently, up to
  # :size) for any still-waiting callers not already covered by an in-flight launch. Launches
  # run in tasks, so the pool stays responsive while Chrome warms up (#22).
  defp dispatch(state) do
    state |> serve_available() |> start_launches()
  end

  # Assign idle browsers to the head of the queue, FIFO, until either runs out.
  defp serve_available(%__MODULE__{available: [browser | rest_avail]} = state) do
    case :queue.out(state.waiting) do
      {{:value, waiter}, rest} ->
        %{state | available: rest_avail, waiting: rest}
        |> assign(browser, waiter)
        |> serve_available()

      {:empty, _} ->
        state
    end
  end

  defp serve_available(state), do: state

  # Start one launch per still-uncovered waiter, bounded by free capacity so
  # count + in-flight launches never exceeds :size. Launches are anonymous: each browser
  # joins `available` on success and the next dispatch serves a waiter FIFO.
  defp start_launches(%__MODULE__{count: count, size: size, launching: launching} = state) do
    capacity = size - count - map_size(launching)
    uncovered = :queue.len(state.waiting) - map_size(launching)
    n = max(min(capacity, uncovered), 0)

    if n > 0, do: Enum.reduce(1..n, state, fn _, acc -> start_one_launch(acc) end), else: state
  end

  # Launch a browser in a monitored task. async_nolink keeps a launch crash from taking the
  # pool down (we get its {:DOWN}); the task start_links the browser then unlinks it so the
  # POOL adopts it on the result — a browser linked to the ephemeral task would lose the
  # pool's crash handling. Keyed by the task ref to correlate the result / crash.
  defp start_one_launch(%__MODULE__{} = state) do
    pool = self()

    task =
      Task.Supervisor.async_nolink(state.task_sup, fn ->
        ProcessLabel.set({:cdp_pool_launch, pool})
        launch(state.start_fun, state.launch_opts, pool)
      end)

    %{state | launching: Map.put(state.launching, task.ref, true)}
  end

  defp launch(start_fun, launch_opts, pool) do
    # Pass `owner: pool` so an adopted browser's owner-death self-reap targets the pool, not
    # this throwaway task (the default Browser honours it; an injected start_fun ignores it).
    case start_fun.(Keyword.put(launch_opts, :owner, pool)) do
      {:ok, browser} = ok ->
        # Detach from this (about-to-exit) task so the pool, not the task, owns the link.
        Process.unlink(browser)
        ok

      other ->
        other
    end
  end

  # Adopt a freshly launched browser: link it to the pool (the task unlinked it), add it to
  # the available set, and dispatch so a waiter is served. If it died in the unlink→adopt
  # gap, alive? routes it through launch_failed rather than handing a dead pid to a waiter;
  # a death right after link/1 surfaces as an {:EXIT, browser, _} the EXIT clause reconciles.
  defp browser_launched(state, browser) do
    if Process.alive?(browser) do
      Process.link(browser)

      dispatch(%{state | count: state.count + 1, available: [browser | state.available]})
    else
      launch_failed(state, :noproc)
    end
  end

  # A launch failed (start error or task crash). Surface the reason to the head waiter ONLY
  # when this failure leaves one uncovered — i.e. fewer in-flight launches remain than queued
  # waiters. If every waiter is still covered by another in-flight launch (e.g. this failed
  # launch's waiter already timed out and left the queue), don't spuriously fail a live
  # waiter — let the other launches serve them. Replying on a genuine shortfall also bounds a
  # relaunch loop on persistent failure (each failure consumes one waiter).
  defp launch_failed(state, reason) do
    if map_size(state.launching) < :queue.len(state.waiting) do
      case :queue.out(state.waiting) do
        {{:value, {from, timer}}, rest} ->
          _ = cancel_timer(timer)
          GenServer.reply(from, {:error, reason})
          dispatch(%{state | waiting: rest})

        {:empty, _} ->
          dispatch(state)
      end
    else
      dispatch(state)
    end
  end

  # Hand `browser` to the waiter under a fresh lease. The lease pid is the rental's
  # identity: it monitors the owner and the browser, and its linked EXIT settles
  # this exact slot. Cancel the wait timer and reply with the lease, not the browser.
  defp assign(state, browser, {from, timer}) do
    owner = elem(from, 0)
    _ = cancel_timer(timer)

    case Lease.start_link(self(), owner, browser) do
      {:ok, lease} ->
        GenServer.reply(from, {:ok, lease})
        %{state | busy: Map.put(state.busy, lease, {browser, owner})}

      {:error, reason} ->
        # The lease itself couldn't start (extreme system limits). Never hand out a
        # browser with no lease guarding it: retire this one and fail the waiter,
        # leaving the queue/dispatch invariants intact for the next caller.
        Logger.error("[CDPEx.Pool] failed to start a lease: #{inspect(reason)}")
        safe_stop(browser)
        GenServer.reply(from, {:error, reason})
        %{state | count: state.count - 1}
    end
  end

  # Unwrap a lease's settle reason (see CDPEx.Lease.stop_reason/1). A clean check-in
  # exits plain :shutdown; anything else carries the end_reason. An exit we don't
  # recognize (e.g. the lease itself crashing) is treated as an untrusted stop.
  defp lease_reason(:shutdown), do: :normal
  defp lease_reason({:shutdown, {:lease_exit, reason}}), do: reason
  defp lease_reason(other), do: {:lease_stopped, other}

  # Settle a lease. Its busy entry (one counted browser) is popped first. Retirement
  # and browser-death paths decrement count here, unconditionally: the popped entry
  # proves the browser occupied a slot. A browser {:EXIT} that landed first already
  # removed it from busy, so settle_lease is never even called for that lease — the
  # two paths can't both decrement the same slot.
  defp settle_lease(state, lease, reason) do
    {{browser, _owner}, busy} = Map.pop(state.busy, lease)
    state = %{state | busy: busy}

    case reason do
      :normal ->
        # Clean check-in. Don't shelf an already-dead browser (its {:EXIT} may not be
        # processed yet): drop it instead; the EXIT path reconciles anything missed.
        if Process.alive?(browser) do
          %{state | available: [browser | state.available]}
        else
          %{state | count: state.count - 1}
        end

      {:abandoned, exit_reason} ->
        # The owner died mid-rental. Its page work may be half-done: never reuse this
        # browser for another job — retire it and let dispatch launch a replacement.
        retire(state, browser, {:abandoned, exit_reason})

      :browser_gone ->
        # The browser died first; free the slot the popped entry was accounting for.
        %{state | count: state.count - 1}

      {:lease_stopped, _} = stop_reason ->
        # The lease stopped for a reason it didn't wrap: don't trust its browser.
        retire(state, browser, stop_reason)

      contamination ->
        # A check-in after contaminate/2: never reuse a browser that may carry a late
        # unowned tab — retire and backfill.
        retire(state, browser, {:contaminated, contamination})
    end
  end

  # Take a browser out of rotation and shrink the count. Called only from
  # settle_lease/2, whose popped busy entry guarantees this browser held exactly one
  # counted slot — so the decrement is unconditional here. Stopping is best-effort.
  defp retire(state, browser, reason) do
    Telemetry.pool_browser_retired(reason)

    safe_stop(browser)
    %{state | count: state.count - 1}
  end

  # Remove a dead browser from wherever it is and shrink the count. A busy browser's
  # lease settles separately; here we only make sure count never counts a corpse.
  defp drop_browser(state, browser) do
    cond do
      Enum.any?(state.busy, fn {_lease, {b, _owner}} -> b == browser end) ->
        busy = Map.reject(state.busy, fn {_lease, {b, _owner}} -> b == browser end)
        %{state | busy: busy, count: state.count - 1}

      browser in state.available ->
        %{state | available: List.delete(state.available, browser), count: state.count - 1}

      true ->
        state
    end
  end

  defp remove_waiter(waiting, from) do
    case List.keytake(:queue.to_list(waiting), from, 0) do
      {{^from, timer}, rest} -> {:ok, timer, :queue.from_list(rest)}
      nil -> :error
    end
  end

  defp arm_timeout(_from, :infinity), do: nil

  defp arm_timeout(from, timeout) when is_integer(timeout) do
    Process.send_after(self(), {:checkout_timeout, from}, max(timeout, 0))
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref)

  defp safe_stop(browser) do
    if Process.alive?(browser), do: Browser.stop(browser)
    :ok
  end
end
