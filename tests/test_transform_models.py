"""Tests for model parsing, DAG resolution and SQL compilation."""

from __future__ import annotations

from pathlib import Path

import pytest

from clickstream.transform.models import (
    Model,
    ModelError,
    compile_sql,
    discover_models,
    load_model,
    resolve_order,
    select_models,
)

PROJECT_MODELS = Path(__file__).resolve().parent.parent / "models"


def write(tmp_path: Path, folder: str, name: str, sql: str) -> Path:
    directory = tmp_path / folder
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.sql"
    path.write_text(sql, encoding="utf-8")
    return path


def _first_statement_keyword(sql: str) -> str:
    """First real SQL word, skipping leading `--` comment lines."""
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            return stripped.split(maxsplit=1)[0].lower()
    return ""


def fake_model(name: str, *, depends_on: tuple[str, ...] = (), schema: str = "mart") -> Model:
    return Model(
        name=name,
        path=Path(f"{name}.sql"),
        schema=schema,
        materialized="table",
        unique_key=None,
        indexes=(),
        description="",
        raw_sql="select 1",
        depends_on=depends_on,
    )


class TestLoadModel:
    def test_defaults_to_a_view(self, tmp_path):
        path = write(tmp_path, "marts", "m", "select 1")
        assert load_model(path, default_schema="mart").materialized == "view"

    def test_reads_the_config_block(self, tmp_path):
        path = write(
            tmp_path,
            "marts",
            "m",
            "{{ config(materialized='table', indexes=['a', 'b, c']) }}\nselect 1",
        )
        model = load_model(path, default_schema="mart")
        assert model.materialized == "table"
        assert model.indexes == ("a", "b, c")

    def test_config_schema_overrides_the_folder_default(self, tmp_path):
        path = write(tmp_path, "marts", "m", "{{ config(schema='stg') }} select 1")
        assert load_model(path, default_schema="mart").schema == "stg"

    def test_relation_is_schema_qualified(self, tmp_path):
        path = write(tmp_path, "marts", "fct_sessions", "select 1")
        assert load_model(path, default_schema="mart").relation == "mart.fct_sessions"

    def test_records_refs_as_dependencies(self, tmp_path):
        path = write(
            tmp_path,
            "marts",
            "m",
            "select * from {{ ref('a') }} join {{ ref('b') }} using (id)",
        )
        assert load_model(path, default_schema="mart").depends_on == ("a", "b")

    def test_repeated_ref_is_recorded_once(self, tmp_path):
        path = write(
            tmp_path,
            "marts",
            "m",
            "select * from {{ ref('a') }} union all select * from {{ ref('a') }}",
        )
        assert load_model(path, default_schema="mart").depends_on == ("a",)

    def test_rejects_unknown_materialization(self, tmp_path):
        path = write(tmp_path, "marts", "m", "{{ config(materialized='cube') }} select 1")
        with pytest.raises(ModelError, match="materialized must be"):
            load_model(path, default_schema="mart")

    def test_rejects_unknown_config_key(self, tmp_path):
        path = write(tmp_path, "marts", "m", "{{ config(partition_by='day') }} select 1")
        with pytest.raises(ModelError, match="unknown config key"):
            load_model(path, default_schema="mart")

    def test_incremental_requires_a_unique_key(self, tmp_path):
        # Without a key an incremental rebuild would duplicate, not update.
        path = write(tmp_path, "marts", "m", "{{ config(materialized='incremental') }} select 1")
        with pytest.raises(ModelError, match="requires a unique_key"):
            load_model(path, default_schema="mart")

    def test_view_cannot_declare_indexes(self, tmp_path):
        path = write(tmp_path, "marts", "m", "{{ config(indexes=['a']) }} select 1")
        with pytest.raises(ModelError, match="view cannot have indexes"):
            load_model(path, default_schema="mart")


class TestDiscoverModels:
    def test_rejects_an_unknown_folder(self, tmp_path):
        write(tmp_path, "scratch", "m", "select 1")
        with pytest.raises(ModelError, match="models must live in"):
            discover_models(tmp_path)

    def test_rejects_duplicate_model_names(self, tmp_path):
        write(tmp_path, "staging", "m", "select 1")
        write(tmp_path, "marts", "m", "select 1")
        with pytest.raises(ModelError, match="duplicate model name"):
            discover_models(tmp_path)

    def test_folder_picks_the_default_schema(self, tmp_path):
        write(tmp_path, "staging", "stg_a", "select 1")
        write(tmp_path, "intermediate", "int_b", "select 1")
        write(tmp_path, "marts", "fct_c", "select 1")
        models = discover_models(tmp_path)
        assert models["stg_a"].schema == "stg"
        assert models["int_b"].schema == "stg"
        assert models["fct_c"].schema == "mart"


class TestResolveOrder:
    def test_dependencies_come_first(self):
        models = {
            "c": fake_model("c", depends_on=("b",)),
            "b": fake_model("b", depends_on=("a",)),
            "a": fake_model("a"),
        }
        assert [m.name for m in resolve_order(models)] == ["a", "b", "c"]

    def test_order_is_stable_for_independent_models(self):
        models = {name: fake_model(name) for name in ("z", "m", "a")}
        assert [m.name for m in resolve_order(models)] == ["a", "m", "z"]

    def test_detects_a_cycle(self):
        models = {
            "a": fake_model("a", depends_on=("b",)),
            "b": fake_model("b", depends_on=("a",)),
        }
        with pytest.raises(ModelError, match="cycle"):
            resolve_order(models)

    def test_missing_dependency_is_reported(self):
        models = {"a": fake_model("a", depends_on=("ghost",))}
        with pytest.raises(ModelError, match="not a model"):
            resolve_order(models)


class TestSelectModels:
    def test_selection_pulls_in_ancestors(self):
        # Running a model without its upstreams would read stale state.
        models = {
            "a": fake_model("a"),
            "b": fake_model("b", depends_on=("a",)),
            "c": fake_model("c", depends_on=("b",)),
            "unrelated": fake_model("unrelated"),
        }
        assert [m.name for m in select_models(models, ["c"])] == ["a", "b", "c"]

    def test_no_selection_builds_everything(self):
        models = {"a": fake_model("a"), "b": fake_model("b")}
        assert len(select_models(models, None)) == 2

    def test_unknown_selection_is_rejected(self):
        with pytest.raises(ModelError, match="unknown model"):
            select_models({"a": fake_model("a")}, ["ghost"])


class TestCompileSql:
    def _compile(self, sql, *, is_incremental=False, variables=None, deps=None):
        model = Model(
            name="target",
            path=Path("target.sql"),
            schema="mart",
            materialized="incremental",
            unique_key="id",
            indexes=(),
            description="",
            raw_sql=sql,
            depends_on=tuple(deps or ()),
        )
        registry = {"target": model}
        for dep in deps or ():
            registry[dep] = fake_model(dep, schema="stg")
        return compile_sql(
            model, registry, is_incremental=is_incremental, variables=variables or {}
        )

    def test_ref_becomes_a_qualified_relation(self):
        out = self._compile("select * from {{ ref('upstream') }}", deps=["upstream"])
        assert out == "select * from stg.upstream"

    def test_source_becomes_a_qualified_relation(self):
        assert self._compile("select * from {{ source('raw', 'events') }}") == (
            "select * from raw.events"
        )

    def test_this_becomes_the_models_own_relation(self):
        assert self._compile("select * from {{ this }}") == "select * from mart.target"

    def test_config_block_is_stripped(self):
        out = self._compile("{{ config(materialized='table') }}\nselect 1")
        assert "config" not in out
        assert out == "select 1"

    def test_trailing_semicolon_is_removed(self):
        assert self._compile("select 1;") == "select 1"

    def test_incremental_block_is_kept_when_incremental(self):
        sql = "select 1 {% if is_incremental() %}where x > 0{% endif %}"
        assert "where x > 0" in self._compile(sql, is_incremental=True)

    def test_incremental_block_is_dropped_otherwise(self):
        # It must be removed, not merely false: the guarded SQL references
        # {{ this }}, which does not exist on a first build, and Postgres
        # resolves relations at parse time.
        sql = "select 1 {% if is_incremental() %}where x > (select max(y) from {{ this }}){% endif %}"
        out = self._compile(sql, is_incremental=False)
        assert "mart.target" not in out
        assert out == "select 1"

    def test_is_incremental_renders_a_boolean(self):
        assert self._compile("select {{ is_incremental() }}", is_incremental=True) == (
            "select true"
        )
        assert self._compile("select {{ is_incremental() }}") == "select false"

    def test_numeric_var_inlines_bare(self):
        out = self._compile(
            "select make_interval(mins => {{ var('t') }})", variables={"t": 30}
        )
        assert out == "select make_interval(mins => 30)"

    def test_string_var_is_quoted_and_escaped(self):
        out = self._compile("select {{ var('s') }}", variables={"s": "it's"})
        assert out == "select 'it''s'"

    def test_undefined_var_is_rejected(self):
        with pytest.raises(ModelError, match="not defined"):
            self._compile("select {{ var('nope') }}")

    def test_unsupported_template_expression_is_rejected(self):
        # Better than shipping `{{ ... }}` to Postgres and getting a syntax
        # error that points at the wrong thing.
        with pytest.raises(ModelError, match="unsupported template"):
            self._compile("select {{ ref_typo('x') }}")


class TestProjectModels:
    """The real models in models/ must parse and form a valid DAG."""

    def test_project_models_resolve(self):
        models = discover_models(PROJECT_MODELS)
        order = [m.name for m in resolve_order(models)]

        assert "stg_events" in models
        # Staging must precede sessionization, which must precede the facts.
        assert order.index("stg_events") < order.index("int_sessionized_events")
        assert order.index("int_sessionized_events") < order.index("fct_sessions")
        assert order.index("fct_sessions") < order.index("fct_funnel_steps")
        assert order.index("fct_funnel_steps") < order.index("agg_funnel_conversion")

    def test_every_project_model_compiles(self):
        models = discover_models(PROJECT_MODELS)
        variables = {"session_timeout_minutes": 30, "funnel_steps": "a,b"}
        for model in models.values():
            for incremental in (False, True):
                sql = compile_sql(
                    model, models, is_incremental=incremental, variables=variables
                )
                assert "{{" not in sql, model.name
                assert "{%" not in sql, model.name
                assert _first_statement_keyword(sql) in ("select", "with"), model.name

    def test_only_stg_events_reads_a_raw_source(self):
        # Everything else must go through ref(), or the DAG would not know
        # about the dependency and could build in the wrong order.
        models = discover_models(PROJECT_MODELS)
        reading_raw = {
            name for name, m in models.items() if "source(" in m.raw_sql
        }
        assert reading_raw == {"stg_events"}
