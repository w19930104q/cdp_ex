defmodule CDPEx.PoolTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias CDPEx.Lease
  alias CDPEx.Pool
  alias CDPEx.TestPoolBrowsers.FakeBrowser
  alias CDPEx.TestPoolBrowsers.PageBrowser

  setup do
    # The pool is start_linked to the test process; trap exits so a pool stop in
    # teardown can't take the test down.
    Process.flag(:trap_exit, true)
    :ok
  end

  # ── basic pool mechanics ──────────────────────────────────────────────────────

  test "checkout lazily launches up to size, then reuses warm browsers" do
    pool = start_pool(size: 2)

    {:ok, l1} = Pool.checkout(pool)
    {:ok, b1} = Lease.browses(l1)
    {:ok, l2} = Pool.checkout(pool)
    {:ok, b2} = Lease.browses(l2)
    assert b1 != b2
    assert :sys.get_state(pool).count == 2

    :ok = Pool.checkin(pool, l1)
    # Reuses the freed browser instead of launching a third.
    assert {:ok, l3} = Pool.checkout(pool)
    assert {:ok, ^b1} = Lease.browses(l3)
    assert :sys.get_state(pool).count == 2
  end

  test "checkout blocks when the pool is exhausted and is served on checkin" do
    pool = start_pool(size: 1)
    {:ok, l1} = Pool.checkout(pool)

    task = Task.async(fn -> Pool.checkout(pool, 1_000) end)
    refute Task.yield(task, 100), "checkout should block while the pool is exhausted"

    :ok = Pool.checkin(pool, l1)
    assert {:ok, _lease} = Task.await(task)
  end

  test "checkout times out when no browser frees up in time" do
    pool = start_pool(size: 1)
    {:ok, _l1} = Pool.checkout(pool)
    assert {:error, :timeout} = Pool.checkout(pool, 100)
  end

  test "with_browser passes the browser pid and checks the lease back in" do
    pool = start_pool(size: 1)
    assert :hello = Pool.with_browser(pool, fn _b -> :hello end)
    assert {:ok, _l} = Pool.checkout(pool, 500)
  end

  test "with_browser ends the lease even when the fun raises" do
    pool = start_pool(size: 1)
    assert_raise RuntimeError, fn -> Pool.with_browser(pool, fn _b -> raise "boom" end) end
    assert {:ok, _l} = Pool.checkout(pool, 500)
  end

  test "with_browser ends the lease even when the fun exits" do
    pool = start_pool(size: 1)
    # `after` runs on an exit too, so the lease is still ended.
    catch_exit(Pool.with_browser(pool, fn _b -> exit(:boom) end))
    assert {:ok, _l} = Pool.checkout(pool, 500)
  end

  # ── lease isolation: owner death ──────────────────────────────────────────────

  test "a checkout owner that crashes has its lease settled without touching other rentals" do
    pool = start_pool(size: 2)
    {:ok, survivor_lease} = Pool.checkout(pool)
    {:ok, survivor} = Lease.browses(survivor_lease)

    parent = self()

    {owner, _ref} =
      spawn_monitor(fn ->
        {:ok, lease} = Pool.checkout(pool)
        {:ok, browser} = Lease.browses(lease)
        send(parent, {:checked_out, lease, browser})
        Process.sleep(:infinity)
      end)

    assert_receive {:checked_out, lease, browser}, 1_000

    assert_retires(pool, :abandoned, fn ->
      Process.exit(owner, :kill)
      assert_down(lease)
    end)

    # The abandoned browser is retired (its stop runs when the pool settles the
    # lease's EXIT) and its slot freed.
    assert eventually(fn -> not Process.alive?(browser) end)
    assert eventually(fn -> :sys.get_state(pool).count == 1 end)

    # The other rental is completely untouched: same lease, same browser.
    assert {:ok, ^survivor} = Lease.browses(survivor_lease)
    assert Process.alive?(survivor)
  end

  # ── lease isolation: browser death ────────────────────────────────────────────

  test "a browser that crashes while busy frees only its own lease and is replaced" do
    pool = start_pool(size: 1)
    {:ok, lease} = Pool.checkout(pool)
    {:ok, browser} = Lease.browses(lease)

    Process.exit(browser, :kill)
    assert_down(lease)
    assert eventually(fn -> :sys.get_state(pool).count == 0 end)

    # A waiter never gets the dead pid: the replacement is a fresh browser.
    assert {:ok, new_lease} = Pool.checkout(pool, 1_000)
    {:ok, new_browser} = Lease.browses(new_lease)
    assert new_browser != browser
    assert Process.alive?(new_browser)
    assert :sys.get_state(pool).count == 1
  end

  test "a crashed available browser is dropped and replaced on the next checkout" do
    pool = start_pool(size: 1)
    {:ok, lease} = Pool.checkout(pool)
    {:ok, browser} = Lease.browses(lease)
    :ok = Pool.checkin(pool, lease)

    Process.exit(browser, :kill)
    assert eventually(fn -> :sys.get_state(pool).count == 0 end)

    assert {:ok, new_lease} = Pool.checkout(pool, 1_000)
    {:ok, new_browser} = Lease.browses(new_lease)
    assert new_browser != browser
  end

  test "a lease whose browser dies reports :noproc from browses/1 instead of a dead pid" do
    pool = start_pool(size: 1)
    {:ok, lease} = Pool.checkout(pool)
    {:ok, browser} = Lease.browses(lease)

    Process.exit(browser, :kill)
    assert eventually(fn -> not Process.alive?(lease) end)
    assert Lease.browses(lease) == {:error, :noproc}
  end

  # ── late-page quarantine (the #145 risk) ─────────────────────────────────────

  test "a timed-out new_page contaminates the lease and retires the browser, never reusing it" do
    # The fake fails new_page with exactly what CDPEx.Browser returns on a stalled
    # page open — the tab may still appear on Chrome afterwards.
    pool =
      start_pool(
        size: 1,
        start_fun: fn _ -> PageBrowser.start_link(new_page: {:error, {:timeout, :new_page}}) end
      )

    parent = self()

    assert_retires(pool, :contaminated, fn ->
      result =
        Pool.with_page(pool, fn _page ->
          send(parent, :fun_should_not_run)
          :ran
        end)

      assert result == {:error, {:timeout, :new_page}}
    end)

    refute_receive :fun_should_not_run
    # The contaminated browser was retired and its slot freed; the next call gets a
    # brand-new browser (a different pid), so a late tab can't bleed across rentals.
    assert eventually(fn -> :sys.get_state(pool).count == 0 end)

    # A fresh backfill browser serves the next rental; the fun's own value is
    # returned (not an :ok tuple), proving the replacement is fully usable.
    assert :ok =
             Pool.with_browser(pool, fn b ->
               assert is_pid(b)
               :ok
             end)
  end

  test "a late page opened after the timeout stays on the retired browser, not the next lease" do
    # new_page sleeps past the with_page deadline, then opens the tab anyway: the
    # real "page arrives late and unowned" shape. with_page's own deadline fires
    # first; we then contaminate explicitly (as the pool would on a true new_page
    # timeout — here simulated deterministically).
    pool =
      start_pool(size: 1, start_fun: fn _ -> PageBrowser.start_link(new_page: {:delay, 200}) end)

    {:ok, lease} = Pool.checkout(pool)
    {:ok, tainted} = Lease.browses(lease)

    # Simulate the caller giving up while new_page is still in flight.
    Lease.contaminate(lease, {:timeout, :new_page})

    assert_retires(pool, :contaminated, fn ->
      Pool.checkin(pool, lease)
    end)

    # The retired browser's stop runs as the pool settles the lease.
    assert eventually(fn -> not Process.alive?(tainted) end)

    # The replacement browser is a distinct process; whatever the tainted browser's
    # late open produced can never be observable through it.
    {:ok, lease2} = Pool.checkout(pool, 1_000)
    {:ok, replacement} = Lease.browses(lease2)
    assert replacement != tainted
    assert :sys.get_state(replacement).open == %{}
  end

  test "a clean with_page keeps the warm browser in rotation (no false retirement)" do
    pool = start_pool(size: 1, start_fun: fn _ -> PageBrowser.start_link([]) end)

    assert :ok =
             Pool.with_page(pool, fn page ->
               assert is_binary(page.target_id)
               :ok
             end)

    # The lease settles asynchronously (its EXIT, not the checkin cast, updates the
    # pool), so wait for the browser to be shelved before reading internal state.
    assert eventually(fn ->
             st = :sys.get_state(pool)
             st.count == 1 and length(st.available) == 1 and map_size(st.busy) == 0
           end)
  end

  # ── checkin idempotence / stale leases ────────────────────────────────────────

  test "checkin is idempotent: a second checkin after settlement is a no-op" do
    pool = start_pool(size: 1)
    {:ok, lease} = Pool.checkout(pool)

    :ok = Pool.checkin(pool, lease)
    assert eventually(fn -> not Process.alive?(lease) end)
    :ok = Pool.checkin(pool, lease)
    assert :sys.get_state(pool).count == 1
  end

  test "a contaminated lease that already ended drops the contaminate cast harmlessly" do
    pool = start_pool(size: 1)
    {:ok, lease} = Pool.checkout(pool)
    :ok = Pool.checkin(pool, lease)
    assert eventually(fn -> not Process.alive?(lease) end)

    # Cast to a dead lease: GenServer.cast never raises, pool state stays coherent.
    :ok = Lease.contaminate(lease, :too_late)
    assert {:ok, _l} = Pool.checkout(pool, 500)
  end

  # ── pool stop ─────────────────────────────────────────────────────────────────

  test "stopping the pool replies to blocked waiters instead of crashing them" do
    pool = start_pool(size: 1)
    {:ok, _l1} = Pool.checkout(pool)

    task = Task.async(fn -> Pool.checkout(pool, 5_000) end)
    refute Task.yield(task, 100), "the second checkout should be blocked"

    Pool.stop(pool)
    assert {:error, :noproc} = Task.await(task)
  end

  test "a pool stop takes its leases down with it" do
    pool = start_pool(size: 1)
    {:ok, lease} = Pool.checkout(pool)
    {:ok, browser} = Lease.browses(lease)

    Pool.stop(pool)
    assert_down(lease)
    assert_down(browser)
  end

  test "child_spec derives its id from :id or :name so multiple pools can be supervised" do
    assert Pool.child_spec([]).id == Pool
    assert Pool.child_spec(name: :fast).id == :fast
    assert Pool.child_spec(id: :custom, name: :fast).id == :custom
  end

  # ── launch lifecycle ──────────────────────────────────────────────────────────

  test "a launch failure surfaces to the checkout caller" do
    pool = start_pool(size: 1, start_fun: fn _ -> {:error, :launch_boom} end)
    assert {:error, :launch_boom} = Pool.checkout(pool, 1_000)
    # The failed launch freed its slot, so a retry attempts a fresh launch (not stuck).
    assert {:error, :launch_boom} = Pool.checkout(pool, 1_000)
    assert Process.alive?(pool)
  end

  test "a launch task crash surfaces its reason as an error and the pool survives" do
    pool = start_pool(size: 1, start_fun: fn _ -> raise "launch crash" end)

    # The launch task crash logs a (expected) SASL report — capture it so it isn't noise.
    {result, _log} = with_log(fn -> Pool.checkout(pool, 1_000) end)

    # The task's :DOWN reason (the raised exception) is propagated, not a bare atom.
    assert {:error, {%RuntimeError{message: "launch crash"}, _stacktrace}} = result
    assert Process.alive?(pool)
  end

  test "the pool stays responsive while a launch is in flight (#22)" do
    # A launch that parks until the test releases it (deterministic, no wall-clock). In the
    # old synchronous pool the GenServer was blocked inside start_fun, so it couldn't process
    # any other message — the second checkout below would hang rather than time out.
    test = self()

    parked_start = fn opts ->
      send(test, {:launching, self()})
      receive do: (:proceed -> :ok)
      FakeBrowser.start_link(opts)
    end

    pool = start_pool(size: 1, start_fun: parked_start)

    slow = Task.async(fn -> Pool.checkout(pool, 5_000) end)
    assert_receive {:launching, launcher}, 1_000

    # The launch is provably still parked (we haven't sent :proceed), yet a second checkout
    # is served its :timeout — the pool is responsive. A synchronous pool would be stuck.
    assert {:error, :timeout} = Pool.checkout(pool, 50)

    # Release the launch; the original caller is served.
    send(launcher, :proceed)
    assert {:ok, _lease} = Task.await(slow, 2_000)
  end

  test "launches browsers concurrently for simultaneous waiters (#22)" do
    # Each launch announces itself then parks. If launches were serial, only one would be in
    # flight at a time; we assert all three are parked at once, proving concurrency.
    test = self()

    parked_start = fn opts ->
      send(test, {:launching, self()})
      receive do: (:proceed -> :ok)
      FakeBrowser.start_link(opts)
    end

    pool = start_pool(size: 3, start_fun: parked_start)

    # Holders keep their browsers checked out (a Task that exits right after checkout would
    # let the pool reclaim its browser on the owner's death and re-hand it to the next
    # waiter — so we'd see fewer than three distinct browsers).
    holders =
      for _ <- 1..3 do
        spawn(fn ->
          {:ok, lease} = Pool.checkout(pool, 3_000)
          {:ok, b} = Lease.browses(lease)
          send(test, {:got, b})
          receive do: (:release -> :ok), after: (5_000 -> :ok)
        end)
      end

    launchers =
      for _ <- 1..3 do
        assert_receive {:launching, pid}, 1_000
        pid
      end

    assert length(Enum.uniq(launchers)) == 3, "expected three concurrent launches"

    Enum.each(launchers, &send(&1, :proceed))

    browsers =
      for _ <- 1..3 do
        assert_receive {:got, b}, 2_000
        b
      end

    assert length(Enum.uniq(browsers)) == 3
    assert :sys.get_state(pool).count == 3

    Enum.each(holders, &send(&1, :release))
  end

  test "the pool stops with an attributable reason if its launch supervisor dies (#22)" do
    pool = start_pool(size: 1)
    ref = Process.monitor(pool)
    task_sup = :sys.get_state(pool).task_sup

    # The pool's stop logs an (expected) termination report — capture it so it isn't noise.
    capture_log(fn ->
      Process.exit(task_sup, :kill)

      # Rather than limping unable to launch, the pool stops so its own supervisor can
      # restart it with a fresh task_sup.
      assert_receive {:DOWN, ^ref, :process, ^pool, {:task_sup_down, :killed}}, 1_000
    end)
  end

  test "a launch that outlives its timed-out waiter becomes a warm spare (#22)" do
    test = self()

    parked_start = fn opts ->
      send(test, {:launching, self()})
      receive do: (:proceed -> :ok)
      FakeBrowser.start_link(opts)
    end

    pool = start_pool(size: 1, start_fun: parked_start)

    # The waiter triggers a launch then times out before it resolves.
    assert {:error, :timeout} = Pool.checkout(pool, 50)
    assert_receive {:launching, launcher}, 1_000

    # The launch completes with no waiter left — its browser is kept as a warm spare, not
    # leaked, and a later checkout reuses it (count stays 1, no second launch).
    send(launcher, :proceed)
    assert {:ok, _lease} = Pool.checkout(pool, 1_000)
    assert :sys.get_state(pool).count == 1
  end

  test "a launch that fails after its waiter timed out is handled without crashing (#22)" do
    test = self()

    parked_fail = fn _opts ->
      send(test, {:launching, self()})
      receive do: (:proceed -> :ok)
      {:error, :late_boom}
    end

    pool = start_pool(size: 1, start_fun: parked_fail)

    assert {:error, :timeout} = Pool.checkout(pool, 50)
    assert_receive {:launching, launcher}, 1_000

    # The failure arrives with no waiter in the queue — the pool absorbs it (no crash) and
    # frees the slot (launching drains back to empty).
    send(launcher, :proceed)
    assert eventually(fn -> :sys.get_state(pool).launching == %{} end)
    assert Process.alive?(pool)
  end

  test "the pool passes itself as :owner so an adopted browser self-reaps against it (#22)" do
    test = self()

    capturing_start = fn opts ->
      send(test, {:launch_opts, opts})
      FakeBrowser.start_link(opts)
    end

    pool = start_pool(size: 1, start_fun: capturing_start)

    {:ok, _lease} = Pool.checkout(pool, 1_000)
    assert_receive {:launch_opts, opts}
    assert opts[:owner] == pool
  end

  test "a launch failure doesn't fail a waiter still covered by another in-flight launch (#22)" do
    test = self()

    controlled = fn opts ->
      send(test, {:launch, self()})

      receive do
        :proceed_ok -> FakeBrowser.start_link(opts)
        {:proceed_error, reason} -> {:error, reason}
      end
    end

    pool = start_pool(size: 2, start_fun: controlled)

    # w2 first (long timeout) — its launch is confirmed in flight before w1 exists, so w1's
    # short timeout can't race ahead of it. w1 then checks out (its launch fires too) and
    # times out, leaving one waiter (w2) but BOTH launches still in flight.
    w2 =
      spawn(fn ->
        send(test, {:w2, Pool.checkout(pool, 3_000)})
        receive do: (:stop -> :ok), after: (5_000 -> :ok)
      end)

    assert_receive {:launch, lb}, 1_000

    w1 =
      spawn(fn ->
        send(test, {:w1, Pool.checkout(pool, 100)})
        receive do: (:stop -> :ok), after: (5_000 -> :ok)
      end)

    assert_receive {:launch, la}, 1_000
    assert_receive {:w1, {:error, :timeout}}, 1_000

    # One launch fails; the other succeeds. w2 must be served by the survivor, not failed by
    # the casualty (pre-refinement it would have been failed to the head of the queue).
    send(la, {:proceed_error, :boom})
    send(lb, :proceed_ok)

    assert_receive {:w2, {:ok, _lease}}, 2_000
    send(w1, :stop)
    send(w2, :stop)
  end

  # ── concurrent backfill under retirement ─────────────────────────────────────

  test "retiring a browser under concurrent waiters backfills without handing out a dead pid" do
    pool = start_pool(size: 1)
    {:ok, lease} = Pool.checkout(pool)
    {:ok, tainted} = Lease.browses(lease)
    parent = self()

    # Long-lived waiter owners: a Task that exits right after checkout would have its
    # lease reclaimed by the owner monitor before we can inspect it. These stay parked
    # until the test releases them, so each lease stays live for the assertions.
    waiters =
      for _ <- 1..2 do
        spawn(fn ->
          {:ok, waiter_lease} = Pool.checkout(pool, 3_000)
          {:ok, browser} = Lease.browses(waiter_lease)
          send(parent, {:served, waiter_lease, browser})
          receive do: (:release -> :ok)
        end)
      end

    # Both waiters queue behind the single occupied slot.
    assert eventually(fn -> :queue.len(:sys.get_state(pool).waiting) == 2 end)

    Lease.contaminate(lease, :simulated)
    Pool.checkin(pool, lease)

    # The first waiter is served immediately by the backfill browser (the retired slot
    # relaunched on dispatch). It must be a live browser that is never the tainted pid.
    assert_receive {:served, l1, b1}, 3_000
    assert Process.alive?(b1)
    refute b1 == tainted

    # Releasing the first rental frees the backfill browser for the second waiter, who
    # has been blocked the whole time. It too gets a live, non-tainted instance — never
    # a dead or retired pid.
    send(Enum.at(waiters, 0), :release)
    Pool.checkin(pool, l1)
    assert_receive {:served, _l2, b2}, 3_000
    assert Process.alive?(b2)
    refute b2 == tainted
    assert b2 == b1, "the second waiter should reuse the warm backfill browser"
    assert :sys.get_state(pool).count == 1

    send(Enum.at(waiters, 1), :release)
  end

  test "a burst of owner deaths retires each lease's own browser and heals back to size" do
    pool = start_pool(size: 3)

    for _ <- 1..3 do
      parent = self()

      spawn(fn ->
        {:ok, lease} = Pool.checkout(pool, 2_000)
        {:ok, browser} = Lease.browses(lease)
        send(parent, {:rental, lease, browser})
        Process.sleep(:infinity)
      end)
    end

    rentals =
      for _ <- 1..3 do
        assert_receive {:rental, lease, browser}, 2_000
        {lease, browser}
      end

    # All three owners die at once; each lease must settle independently.
    for {lease, browser} <- rentals do
      owner = :sys.get_state(lease).owner
      Process.exit(owner, :kill)
      assert_down(lease)
      assert eventually(fn -> not Process.alive?(browser) end)
    end

    # The pool healed: three fresh browsers are serviceable again.
    browsers =
      for _ <- 1..3 do
        {:ok, l} = Pool.checkout(pool, 3_000)
        {:ok, b} = Lease.browses(l)
        b
      end

    assert length(Enum.uniq(browsers)) == 3
    assert :sys.get_state(pool).count == 3
  end

  # ── telemetry ─────────────────────────────────────────────────────────────────

  test "retirement emits [:cdp_ex, :pool, :browser_retired] with the reason" do
    attach([[:cdp_ex, :pool, :browser_retired]])
    pool = start_pool(size: 1)
    {:ok, lease} = Pool.checkout(pool)

    Lease.contaminate(lease, {:timeout, :new_page})
    Pool.checkin(pool, lease)

    assert_receive {:telemetry, [:cdp_ex, :pool, :browser_retired], _measurements,
                    %{reason: {:contaminated, {:timeout, :new_page}}}},
                   1_000
  end

  test "a clean check-in emits no retirement event" do
    attach([[:cdp_ex, :pool, :browser_retired]])
    pool = start_pool(size: 1)
    {:ok, lease} = Pool.checkout(pool)
    Pool.checkin(pool, lease)

    refute_receive {:telemetry, [:cdp_ex, :pool, :browser_retired], _, _}, 200
  end

  # ── helpers ───────────────────────────────────────────────────────────────────

  defp start_pool(opts) do
    {:ok, pool} =
      opts |> Keyword.put_new(:start_fun, &FakeBrowser.start_link/1) |> Pool.start_link()

    on_exit(fn -> stop_quietly(pool) end)
    pool
  end

  defp stop_quietly(pool) do
    if Process.alive?(pool), do: Pool.stop(pool)
  catch
    :exit, _ -> :ok
  end

  defp assert_down(pid) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000
  end

  # Run `fun` and assert a browser_retired event with the given tagged reason
  # (`:contaminated` / `:abandoned` / `:lease_stopped`) fires while it runs.
  defp assert_retires(pool, tag, fun) when is_function(fun, 0) do
    attach([[:cdp_ex, :pool, :browser_retired]], "retire-#{:erlang.unique_integer([:positive])}")
    fun.()

    assert_receive {:telemetry, [:cdp_ex, :pool, :browser_retired], _m, %{reason: reason}}, 1_000

    assert is_tuple(reason) and elem(reason, 0) == tag,
           "expected a #{inspect(tag)} retirement, got #{inspect(reason)}"

    # Drain any additional retirement events from earlier assertions in this test.
    drain_retirements()
    pool
  end

  defp drain_retirements do
    receive do
      {:telemetry, [:cdp_ex, :pool, :browser_retired], _, _} -> drain_retirements()
    after
      0 -> :ok
    end
  end

  defp attach(events, id \\ nil) do
    id = id || "pool-test-#{:erlang.unique_integer([:positive])}"
    :telemetry.attach_many(id, events, &__MODULE__.forward/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
  end

  @doc false
  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end

  defp eventually(fun, retries \\ 100) do
    cond do
      fun.() ->
        true

      retries == 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(fun, retries - 1)
    end
  end
end
