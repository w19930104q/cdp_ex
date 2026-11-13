defmodule CDPEx.Lease do
  @moduledoc """
  One isolated, self-healing lease of a pooled `CDPEx.Browser`.

  A lease is the lifecycle boundary around a single `CDPEx.Pool.checkout/2`:
  the pool starts one `Lease` per checkout, hands its pid (not the browser pid)
  to the caller, and the lease owns exactly that checkout's fate. It is linked
  to the pool and monitors both the **owner** (the checking-out process) and the
  **browser**, so whichever ends first settles the lease without touching any
  other checkout:

    * **Owner death / `done/1`** — the lease ends. On a clean check-in the
      browser goes back to the pool; if the lease was marked suspicious
      (`contaminate/2`, or the owner died while still holding it), the browser
      is retired instead.
    * **Browser death** — the lease ends immediately with `:browser_gone`;
      nobody keeps a handle to a dead browser, and the pool replaces it.
    * **Pool death** — the lease's own link takes it down with the pool; its
      browser was started with the pool as `:owner`, so the browser self-reaps
      (and reaps Chrome) rather than orphaning it.

  The browser is **monitored, not linked** by the lease: a browser crash must
  settle this lease, never propagate as an exit signal into either the owner or
  the pool as a linked-caller fault. The pool itself links the browser.

  A lease is also what stops a *late* page from leaking into the next job:
  `new_page/2` that times out can still open its tab afterwards on the same
  browser. That browser is therefore contaminated — `Pool.with_page/3` calls
  `contaminate/2` on the timeout — and is retired at check-in even though the
  late tab has no owner. The next lease always lands on a different browser.
  """

  use GenServer

  alias CDPEx.ProcessLabel

  @call_timeout 5_000

  defstruct [:owner, :owner_ref, :browser, :browser_ref, :pool, :contamination]

  @type t :: %__MODULE__{}

  @typedoc """
  Why a lease ended. Delivered to the pool as the inner reason of the lease's
  linked `:EXIT` — every lease stop is wrapped as `:shutdown` or
  `{:shutdown, {:lease_exit, end_reason}}` so GenServer stays silent, and the
  pool's `lease_reason/1` unwraps it.

    * `:normal` — a clean `done/1` check-in; the browser returns to the pool
    * `{:abandoned, reason}` — the owner died while still holding the lease;
      its browser is retired
    * `:browser_gone` — the browser stopped/crashed during the lease
    * any other term — a contamination reason latched by `contaminate/2`
      before check-in (e.g. `{:timeout, :new_page}`); the browser is retired

  `:normal` is a deliberate, healthy stop; the rest are fault-driven and keep
  their reasons loud in the `[:cdp_ex, :pool, :browser_retired]` telemetry.
  """
  @type end_reason ::
          :normal
          | {:abandoned, term()}
          | :browser_gone
          | term()

  # ── Public API ──────────────────────────────────────────────────────────────

  @doc """
  Starts a lease for `browser` on behalf of `owner`, linked to `pool`.

  The pool is the only intended caller. The lease monitors `owner` and
  `browser`; its own `:DOWN` (delivered to `pool`) settles the checkout.
  """
  @spec start_link(pid(), pid(), pid()) :: GenServer.on_start()
  def start_link(pool, owner, browser) do
    GenServer.start_link(__MODULE__, {pool, owner, browser})
  end

  @doc """
  Returns the leased browser pid, or `{:error, :noproc}` if the lease already
  ended (e.g. the browser died between the checkout reply and the first call).
  """
  @spec browses(pid()) :: {:ok, pid()} | {:error, :noproc}
  def browses(lease) do
    {:ok, GenServer.call(lease, :browses, @call_timeout)}
  catch
    :exit, {reason, {GenServer, :call, _}} when reason != :calling_self ->
      {:error, :noproc}
  end

  @doc """
  Ends the lease normally — the browser returns to the pool unless the lease
  was contaminated. A no-op if the lease has already ended (the cast is simply
  dropped).
  """
  @spec done(pid()) :: :ok
  def done(lease), do: GenServer.cast(lease, :done)

  @doc """
  Marks the leased browser suspect so it is retired instead of reused at
  check-in. `reason` is reported in telemetry.

  The canonical use is a timed-out `new_page/2`: the browser may still open the
  page later with no owner to close it, so reusing it could hand the next job a
  browser carrying a stray tab (e.g. a stale login page). Retiring it isolates
  the blast radius to this lease; the pool launches a replacement.
  """
  @spec contaminate(pid(), term()) :: :ok
  def contaminate(lease, reason \\ :suspect), do: GenServer.cast(lease, {:contaminate, reason})

  # ── GenServer callbacks ─────────────────────────────────────────────────────

  @impl true
  def init({pool, owner, browser}) do
    # Trap exits so the pool's link arrives as a message (our {:EXIT, pool, _}
    # clause stops us deliberately) instead of taking the default link action.
    Process.flag(:trap_exit, true)
    ProcessLabel.set({:cdp_pool_lease, browser})

    owner_ref = Process.monitor(owner)
    browser_ref = Process.monitor(browser)

    {:ok,
     %__MODULE__{
       pool: pool,
       owner: owner,
       owner_ref: owner_ref,
       browser: browser,
       browser_ref: browser_ref
     }}
  end

  @impl true
  def handle_call(:browses, _from, %{browser: browser} = state) do
    {:reply, browser, state}
  end

  @impl true
  def handle_cast(:done, state) do
    # A deliberate check-in is a healthy stop even when contaminated. Every lease
    # stop is wrapped in :shutdown so GenServer logs no termination report for an
    # expected lifecycle event (a bare non-:normal/non-:shutdown reason would be
    # reported as a crash on every check-in); the pool unwraps the inner reason.
    {:stop, stop_reason(state.contamination || :normal), state}
  end

  def handle_cast({:contaminate, reason}, state) do
    # Keep the first reason: contamination is a one-way latch.
    {:noreply, %{state | contamination: state.contamination || reason}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{owner_ref: ref} = state) do
    # The checking-out owner died without ending the lease. Its browser must
    # never serve another job as if nothing happened: retire it (the owner's
    # own page work may be half-done), and let the pool backfill.
    {:stop, stop_reason({:abandoned, reason}), state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{browser_ref: ref} = state) do
    # The browser stopped/crashed mid-lease. The pool gets its own link EXIT and
    # reconciles count; our exit tells it the busy slot is gone too.
    {:stop, stop_reason(:browser_gone), state}
  end

  def handle_info({:EXIT, pool, reason}, %{pool: pool} = state) do
    # Our pool went down. Stop with the same reason: a hard-killed pool takes us
    # with it. The browser's own :owner is the pool, so the browser self-reaps
    # (terminate/2 reaps Chrome) — nothing is orphaned.
    {:stop, reason, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Lease exits are deliberate lifecycle settlements, not crashes: wrap them in
  # :shutdown (silent under OTP's error logger) while keeping the inner end_reason
  # intact for the pool's pattern matching.
  defp stop_reason(:normal), do: :shutdown
  defp stop_reason(reason), do: {:shutdown, {:lease_exit, reason}}

  @impl true
  def terminate(_reason, state) do
    # Monitors are one-shot; flush any :DOWN that raced the stop so the pool's
    # mailbox never carries a stale monitor message for a lease it is removing.
    if state.owner_ref, do: Process.demonitor(state.owner_ref, [:flush])
    if state.browser_ref, do: Process.demonitor(state.browser_ref, [:flush])
    :ok
  end
end
