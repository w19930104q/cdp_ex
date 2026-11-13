defmodule CDPEx.TestPoolBrowsers do
  @moduledoc false
  # Pool test doubles, kept in test/support so every test module can reference
  # them without relying on a sibling module's nested defmodule being loaded.

  alias CDPEx.Page

  # A trivial GenServer stand-in for CDPEx.Browser: Pool's Browser.stop/1
  # (a GenServer.stop) works on it, and it needs no CDP behavior — the pool only
  # launches, hands it out and stops it.
  defmodule FakeBrowser do
    @moduledoc false
    use GenServer

    def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, :ok)
    @impl true
    def init(:ok), do: {:ok, :ok}
  end

  # A richer fake that tracks the pages it "opened", so a test can prove a browser
  # retired after a timed-out new_page can never hand a late page to another lease.
  # `new_page: {:error, reason}` makes new_page fail; `new_page: {:delay, ms}` makes
  # it sleep first (the delay stands in for Chrome stalling past a deadline).
  defmodule PageBrowser do
    @moduledoc false
    use GenServer

    def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(opts), do: {:ok, %{opts: opts, open: %{}, next: 1}}

    @impl true
    def handle_call({:new_page, _opts}, _from, state) do
      case Map.get(state.opts, :new_page) do
        {:error, reason} ->
          {:reply, {:error, reason}, state}

        {:delay, ms} ->
          Process.sleep(ms)
          reply_page(state)

        _ ->
          reply_page(state)
      end
    end

    def handle_call({:close_page, %Page{target_id: tid}}, _from, state) do
      case Map.pop(state.open, tid) do
        {nil, _} ->
          {:reply, {:error, :unknown_page}, state}

        {_conn, open} ->
          {:reply, :ok, %{state | open: open}}
      end
    end

    defp reply_page(state) do
      conn = spawn(fn -> receive do: (:close -> :ok) end)
      tid = "T#{state.next}"
      page = %Page{browser: self(), conn: conn, target_id: tid}

      {:reply, {:ok, page}, %{state | open: Map.put(state.open, tid, conn), next: state.next + 1}}
    end
  end
end
