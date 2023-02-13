defmodule Commanded.Projections.ProjectionAssertions do
  import ExUnit.Assertions

  alias Commanded.Projections.Repo

  def assert_projections(schema, expected) do
    actual = Repo.all(schema) |> pluck(:name)

    assert actual == expected
  end

  def assert_seen_event(projection_name, projection_partition_key \\ "", expected_last_seen)
      when is_binary(projection_name) and is_binary(projection_partition_key) and
             is_integer(expected_last_seen) do
    assert last_seen_event(projection_name, projection_partition_key) == expected_last_seen
  end

  def last_seen_event(projection_name, projection_partition_key \\ "")
      when is_binary(projection_name) and is_binary(projection_partition_key) do
    sql =
      "SELECT last_seen_event_number from projection_versions where projection_name = $1 and projection_partition_key = $2"

    case Ecto.Adapters.SQL.query(Repo, sql, [projection_name, projection_partition_key]) do
      {:ok, %{num_rows: 0}} -> nil
      {:ok, %{rows: [[last_seen]], num_rows: 1}} -> last_seen
    end
  end

  defp pluck(enumerable, field) do
    Enum.map(enumerable, &Map.get(&1, field))
  end
end
