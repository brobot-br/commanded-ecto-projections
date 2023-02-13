defmodule Commanded.Projections.ConcurrentEctoProjectionTest do
  use ExUnit.Case

  import Commanded.Projections.ProjectionAssertions

  alias Commanded.Projections.Events.{AnEvent, AnotherEvent, ErrorEvent, IgnoredEvent}
  alias Commanded.Projections.Projection
  alias Commanded.Projections.Repo

  defmodule ConcurrentProjector do
    use Commanded.Projections.Ecto,
      application: TestApplication,
      name: "Projector",
      concurrency: 2

    def partition_by(_event, %{stream_id: stream_id}), do: stream_id

    project %AnEvent{name: name}, _metadata, fn multi ->
      Ecto.Multi.insert(multi, :my_projection, %Projection{name: name})
    end

    project %AnotherEvent{name: name}, fn multi ->
      Ecto.Multi.insert(multi, :my_projection, %Projection{name: name})
    end

    project %ErrorEvent{}, fn multi ->
      Ecto.Multi.error(multi, :my_projection, :failure)
    end
  end

  setup do
    start_supervised!(TestApplication)
    Ecto.Adapters.SQL.Sandbox.checkout(Repo)
  end

  test "should handle a projected event" do
    assert :ok ==
             ConcurrentProjector.handle(%AnEvent{}, %{
               handler_name: "Projector",
               stream_id: "1",
               event_number: 1
             })

    assert_projections(Projection, ["AnEvent"])
    assert_seen_event("Projector", "1", 1)
  end

  test "should handle two different types of projected events" do
    assert :ok ==
             ConcurrentProjector.handle(%AnEvent{}, %{
               handler_name: "Projector",
               stream_id: "1",
               event_number: 1
             })

    assert :ok ==
             ConcurrentProjector.handle(%AnotherEvent{}, %{
               handler_name: "Projector",
               stream_id: "2",
               event_number: 2
             })

    assert_projections(Projection, ["AnEvent", "AnotherEvent"])
    assert_seen_event("Projector", "1", 1)
    assert_seen_event("Projector", "2", 2)
  end

  test "should ignore already projected event" do
    assert :ok ==
             ConcurrentProjector.handle(%AnEvent{}, %{
               handler_name: "Projector",
               stream_id: "1",
               event_number: 1
             })

    assert :ok ==
             ConcurrentProjector.handle(%AnEvent{}, %{
               handler_name: "Projector",
               stream_id: "1",
               event_number: 1
             })

    assert_projections(Projection, ["AnEvent"])
    assert_seen_event("Projector", "1", 1)

    assert :ok ==
             ConcurrentProjector.handle(%AnEvent{}, %{
               handler_name: "Projector",
               stream_id: "2",
               event_number: 2
             })

    assert :ok ==
             ConcurrentProjector.handle(%AnEvent{}, %{
               handler_name: "Projector",
               stream_id: "2",
               event_number: 2
             })

    assert_projections(Projection, ["AnEvent", "AnEvent"])
    assert_seen_event("Projector", "2", 2)
  end

  test "should ignore unprojected event" do
    assert :ok == ConcurrentProjector.handle(%IgnoredEvent{}, %{event_number: 1})

    assert_projections(Projection, [])
  end

  test "should ignore unprojected events amongst projections" do
    assert :ok ==
             ConcurrentProjector.handle(%AnEvent{}, %{
               handler_name: "Projector",
               stream_id: "1",
               event_number: 1
             })

    assert :ok ==
             ConcurrentProjector.handle(%IgnoredEvent{}, %{
               handler_name: "Projector",
               stream_id: "1",
               event_number: 2
             })

    assert :ok ==
             ConcurrentProjector.handle(%AnotherEvent{}, %{
               handler_name: "Projector",
               stream_id: "1",
               event_number: 3
             })

    assert :ok ==
             ConcurrentProjector.handle(%IgnoredEvent{}, %{
               handler_name: "Projector",
               stream_id: "1",
               event_number: 4
             })

    assert_projections(Projection, ["AnEvent", "AnotherEvent"])
    assert_seen_event("Projector", "1", 3)
  end

  test "should return an error on failure" do
    assert {:error, :failure} ==
             ConcurrentProjector.handle(%ErrorEvent{}, %{
               handler_name: "Projector",
               stream_id: "1",
               event_number: 1
             })

    assert_projections(Projection, [])
  end
end
