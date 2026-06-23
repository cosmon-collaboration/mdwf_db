"""Tests for defaults v2: per-param storage, dry-run, update, staleness."""

from MDWFutils.jobs.schema import ContextParam, storable_params
from MDWFutils.cli.components import ParameterManager

from tests.conftest import FakeBackend, make_ensemble


def _command_args(**overrides):
    from types import SimpleNamespace

    args = {
        "ensemble": 1,
        "input_params": "",
        "job_params": "",
        "params": False,
        "params_variant": None,
        "no_defaults": False,
        "dry_run": False,
        "update": False,
        "force": False,
        "output_file": None,
    }
    args.update(overrides)
    return SimpleNamespace(**args)


def _param_line(output: str, name: str) -> str:
    for line in output.splitlines():
        if line.startswith(f"{name} "):
            return line
    raise AssertionError(f"No output row for {name!r}:\n{output}")


# ---------------------------------------------------------------------------
# storable_params
# ---------------------------------------------------------------------------


class TestStorableParams:
    def test_filters_non_storable(self):
        schema = [
            ContextParam("a", str, default="1"),
            ContextParam("b", int, storable=False),
            ContextParam("c", str, default="3", storable=False),
            ContextParam("d", float, default=4.0),
        ]
        result = storable_params(schema)
        names = [p.name for p in result]
        assert names == ["a", "d"]

    def test_empty_schema(self):
        assert storable_params([]) == []
        assert storable_params(None) == []

    def test_all_storable(self):
        schema = [
            ContextParam("x", int, default=1),
            ContextParam("y", str, default="foo"),
        ]
        assert len(storable_params(schema)) == 2

    def test_all_non_storable(self):
        schema = [
            ContextParam("x", int, storable=False),
            ContextParam("y", str, storable=False),
        ]
        assert storable_params(schema) == []


# ---------------------------------------------------------------------------
# FakeBackend ensemble_defaults methods
# ---------------------------------------------------------------------------


class TestFakeBackendDefaults:
    def test_get_empty(self):
        fb = FakeBackend()
        result = fb.get_ensemble_defaults(1, "hmc-script", "gpu")
        assert result == {"input_params": {}, "job_params": {}}

    def test_set_and_get(self):
        fb = FakeBackend()
        fb.set_ensemble_defaults(
            1, "hmc-script", "gpu",
            {"trajL": "0.5", "lvl_sizes": "9,1,1"},
            {"nodes": "4", "time_limit": "04:00:00"},
        )
        result = fb.get_ensemble_defaults(1, "hmc-script", "gpu")
        assert result["input_params"]["trajL"] == "0.5"
        assert result["job_params"]["nodes"] == "4"

    def test_delete(self):
        fb = FakeBackend()
        fb.set_ensemble_defaults(1, "hmc-script", "gpu", {"a": "1"}, {"b": "2"})
        assert fb.delete_ensemble_defaults(1, "hmc-script", "gpu") is True
        assert fb.delete_ensemble_defaults(1, "hmc-script", "gpu") is False

    def test_list_all(self):
        fb = FakeBackend()
        fb.set_ensemble_defaults(1, "hmc-script", "gpu", {"a": "1"}, {})
        fb.set_ensemble_defaults(1, "hmc-script", "cpu", {"a": "2"}, {})
        fb.set_ensemble_defaults(2, "hmc-script", "gpu", {"c": "3"}, {})
        results = fb.list_ensemble_defaults(1)
        assert len(results) == 2
        cmds = [r["command"] for r in results]
        assert "hmc-script" in cmds

    def test_list_filtered_by_command(self):
        fb = FakeBackend()
        fb.set_ensemble_defaults(1, "hmc-script", "gpu", {"a": "1"}, {})
        fb.set_ensemble_defaults(1, "mres-script", "gpu", {"b": "2"}, {})
        results = fb.list_ensemble_defaults(1, "hmc-script")
        assert len(results) == 1
        assert results[0]["command"] == "hmc-script"
# ---------------------------------------------------------------------------
# ParameterManager ensemble_defaults
# ---------------------------------------------------------------------------


class TestParameterManagerDefaults:
    def test_load_empty(self):
        fb = FakeBackend()
        pm = ParameterManager(fb)
        result = pm.load_ensemble_defaults(1, "hmc-script", "gpu")
        assert result == {"input_params": {}, "job_params": {}}

    def test_save_and_load(self):
        fb = FakeBackend()
        pm = ParameterManager(fb)
        pm.save_ensemble_defaults(
            1, "hmc-script", "gpu",
            {"trajL": "0.5"},
            {"nodes": "4"},
        )
        result = pm.load_ensemble_defaults(1, "hmc-script", "gpu")
        assert result["input_params"]["trajL"] == "0.5"
        assert result["job_params"]["nodes"] == "4"

    def test_delete(self):
        fb = FakeBackend()
        pm = ParameterManager(fb)
        pm.save_ensemble_defaults(1, "hmc-script", "gpu", {"a": "1"}, {})
        assert pm.delete_ensemble_defaults(1, "hmc-script", "gpu") is True
        assert pm.delete_ensemble_defaults(1, "hmc-script", "gpu") is False

    def test_list(self):
        fb = FakeBackend()
        pm = ParameterManager(fb)
        pm.save_ensemble_defaults(1, "hmc-script", "gpu", {"a": "1"}, {})
        pm.save_ensemble_defaults(1, "mres-script", "gpu", {"b": "2"}, {})
        results = pm.list_ensemble_defaults(1, "hmc-script")
        assert len(results) == 1
        assert results[0]["command"] == "hmc-script"
# ---------------------------------------------------------------------------
# BaseCommand execute flow: dry-run, update, staleness, no-defaults
# ---------------------------------------------------------------------------


class TestBaseCommandDefaults:
    """Test the defaults loading/saving flow in BaseCommand.execute()."""

    def _make_cmd(self, backend):
        """Create a minimal command for testing."""
        from MDWFutils.cli.command import BaseCommand
        from MDWFutils.jobs.hmc import HMCGPUContextBuilder, HMCXMLContextBuilder

        class TestCmd(BaseCommand):
            name = "test-cmd"
            job_builder_class = HMCGPUContextBuilder
            input_builder_class = HMCXMLContextBuilder
            default_variant = "gpu"

        return TestCmd(backend)

    def _make_ensemble(self, tmp_path):
        """Create a minimal ensemble for testing."""
        return make_ensemble(tmp_path)

    def test_dry_run_shows_sources(self, tmp_path, capsys):
        """--dry-run should print param sources and exit 0."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        cmd = self._make_cmd(fb)

        args = _command_args(
            input_params="trajL=0.5 n_trajec=10 lvl_sizes=9,1,1",
            job_params="nodes=4",
            dry_run=True,
        )
        result = cmd.execute(args)
        assert result == 0
        out = capsys.readouterr()
        assert "trajL" in out.out
        assert "Source" in out.out

    def test_no_defaults_skips_db(self, tmp_path, capsys):
        """--no-defaults should not load DB defaults."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        # Pre-populate DB defaults
        fb.set_ensemble_defaults(1, "test-cmd", "gpu", {"trajL": "999"}, {})
        cmd = self._make_cmd(fb)

        args = _command_args(
            input_params="trajL=0.5 n_trajec=10 lvl_sizes=9,1,1",
            no_defaults=True,
            dry_run=True,
        )
        result = cmd.execute(args)
        assert result == 0
        out = capsys.readouterr()
        # trajL should show CLI override, not DB default
        assert "0.5" in out.out

    def test_update_saves_merged_defaults(self, tmp_path):
        """--update should save merged params back as defaults."""
        ens = self._make_ensemble(tmp_path)
        ens["hmc_paths"] = {"exec_path": "/fake/hmc", "bind_script_gpu": "/fake/bind"}
        fb = FakeBackend({1: ens})
        cmd = self._make_cmd(fb)

        args = _command_args(
            input_params="trajL=0.5 n_trajec=10 lvl_sizes=9,1,1",
            job_params="nodes=4",
            update=True,
        )
        # Write to temp dir to avoid ensemble dir issues
        result = cmd.execute(args)
        assert result == 0

        # Verify defaults were saved
        defaults = fb.get_ensemble_defaults(1, "test-cmd", "gpu")
        assert defaults["input_params"]["trajL"] == "0.5"
        assert defaults["job_params"]["nodes"] == "4"

    def test_staleness_warning_when_cli_differs(self, tmp_path, capfd):
        """CLI overrides differing from DB defaults should warn."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        fb.set_ensemble_defaults(1, "test-cmd", "gpu", {"trajL": "1.0"}, {})
        cmd = self._make_cmd(fb)

        args = _command_args(
            input_params="trajL=0.5 n_trajec=10 lvl_sizes=9,1,1",
            dry_run=True,
        )
        cmd.execute(args)
        out = capfd.readouterr()
        assert "WARNING" in out.err
        assert "trajL" in out.err

    def test_force_suppresses_staleness(self, tmp_path, capfd):
        """--force should suppress staleness warnings."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        fb.set_ensemble_defaults(1, "test-cmd", "gpu", {"trajL": "1.0"}, {})
        cmd = self._make_cmd(fb)

        args = _command_args(
            input_params="trajL=0.5 n_trajec=10 lvl_sizes=9,1,1",
            dry_run=True,
            force=True,
        )
        cmd.execute(args)
        out = capfd.readouterr()
        assert "WARNING" not in out.err

    def test_update_suppresses_staleness(self, tmp_path, capfd):
        """--update should also suppress staleness warnings."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        fb.set_ensemble_defaults(1, "test-cmd", "gpu", {"trajL": "1.0"}, {})
        cmd = self._make_cmd(fb)

        args = _command_args(
            input_params="trajL=0.5 n_trajec=10 lvl_sizes=9,1,1",
            update=True,
        )
        cmd.execute(args)
        out = capfd.readouterr()
        assert "WARNING" not in out.err

    def test_update_excludes_non_storable_params(self, tmp_path):
        """--update should not save non-storable params (e.g. config_start)."""
        ens = self._make_ensemble(tmp_path)
        ens["hmc_paths"] = {"exec_path": "/fake/hmc", "bind_script_gpu": "/fake/bind"}
        fb = FakeBackend({1: ens})
        cmd = self._make_cmd(fb)

        args = _command_args(
            input_params="trajL=0.5 n_trajec=10 lvl_sizes=9,1,1 config_start=5",
            job_params="nodes=4 gres=gpu:1",
            update=True,
        )
        cmd.execute(args)
        defaults = fb.get_ensemble_defaults(1, "test-cmd", "gpu")
        # config_start is storable=False, should not be in saved defaults
        assert "config_start" not in defaults["input_params"]
        # gres is storable=False, should not be in saved defaults
        assert "gres" not in defaults["job_params"]
        # Storable params should be present
        assert defaults["input_params"]["trajL"] == "0.5"
        assert defaults["job_params"]["nodes"] == "4"

    def test_dry_run_lists_missing_required_params(self, tmp_path, capsys):
        """--dry-run should show missing required params instead of omitting them."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        cmd = self._make_cmd(fb)

        result = cmd.execute(_command_args(dry_run=True))

        assert result == 0
        out = capsys.readouterr()
        for name in ("n_trajec", "trajL", "lvl_sizes"):
            line = _param_line(out.out, name)
            assert "<required>" in line
            assert "Missing required" in line

    def test_dry_run_uses_alias_sources(self, tmp_path, capsys):
        """A value supplied through an alias should still show as a CLI override."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        cmd = self._make_cmd(fb)

        result = cmd.execute(
            _command_args(
                input_params="Trajectories=10 trajL=0.5 lvl_sizes=9,1,1",
                dry_run=True,
            )
        )

        assert result == 0
        out = capsys.readouterr()
        line = _param_line(out.out, "n_trajec")
        assert "10" in line
        assert "CLI override" in line

    def test_staleness_warning_uses_aliases(self, tmp_path, capsys):
        """Alias overrides should be compared to saved canonical defaults."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        fb.set_ensemble_defaults(1, "test-cmd", "gpu", {"n_trajec": "5"}, {})
        cmd = self._make_cmd(fb)

        result = cmd.execute(
            _command_args(
                input_params="Trajectories=10 trajL=0.5 lvl_sizes=9,1,1",
                dry_run=True,
            )
        )

        assert result == 0
        out = capsys.readouterr()
        assert "n_trajec: CLI=10, saved=5" in out.err

    def test_dry_run_rejects_invalid_types_in_relaxed_mode(self, tmp_path, capsys):
        """Relaxed dry-run should not hide type errors."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        cmd = self._make_cmd(fb)

        result = cmd.execute(_command_args(job_params="nodes=bad", dry_run=True))

        assert result == 1
        out = capsys.readouterr()
        assert "nodes: expected int" in out.err

    def test_dry_run_rejects_invalid_choices_in_relaxed_mode(self, tmp_path, capsys):
        """Relaxed dry-run should not hide choice errors."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        cmd = self._make_cmd(fb)

        result = cmd.execute(_command_args(input_params="mode=bad", dry_run=True))

        assert result == 1
        out = capsys.readouterr()
        assert "mode must be one of" in out.err

    def test_dry_run_hmc_subcommand_loads_hmc_script_defaults(self, tmp_path, capsys):
        """HMC variants should use hmc-script plus variant for defaults."""
        from MDWFutils.cli.commands.hmc_script import HMCGPUCommand

        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        fb.set_ensemble_defaults(
            1,
            "hmc-script",
            "gpu",
            {"n_trajec": "10", "trajL": "0.5", "lvl_sizes": "9,1,1"},
            {"nodes": "4"},
        )
        cmd = HMCGPUCommand(fb)

        result = cmd.execute(_command_args(dry_run=True))

        assert result == 0
        out = capsys.readouterr()
        assert "Command: hmc-script" in out.out
        assert "n_trajec" in out.out
        assert "0.5" in out.out
        assert "nodes" in out.out
        assert "4" in out.out

    def test_dry_run_hmc_subcommand_loads_legacy_builder_defaults(self, tmp_path, capsys):
        """Existing hmc_gpu defaults should still load after canonicalization."""
        from MDWFutils.cli.commands.hmc_script import HMCGPUCommand

        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        fb.set_ensemble_defaults(
            1,
            "hmc_gpu",
            "gpu",
            {"n_trajec": "10", "trajL": "0.5", "lvl_sizes": "9,1,1"},
            {"nodes": "4"},
        )
        cmd = HMCGPUCommand(fb)

        result = cmd.execute(_command_args(dry_run=True))

        assert result == 0
        out = capsys.readouterr()
        assert "Command: hmc-script" in out.out
        assert "0.5" in out.out
        assert "4" in out.out

    def test_update_can_save_partial_defaults_without_generation(self, tmp_path):
        """--update should save partial defaults when required params are missing."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        cmd = self._make_cmd(fb)

        result = cmd.execute(_command_args(job_params="nodes=4", update=True))

        assert result == 0
        defaults = fb.get_ensemble_defaults(1, "test-cmd", "gpu")
        assert defaults["job_params"]["nodes"] == "4"
        assert "n_trajec" not in defaults["input_params"]

    def test_update_saves_defaults_before_generation_validation(self, tmp_path):
        """--update should persist params even if script generation later fails."""
        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        cmd = self._make_cmd(fb)

        result = cmd.execute(
            _command_args(
                input_params="trajL=0.5 n_trajec=10 lvl_sizes=9,1,1",
                job_params="nodes=4",
                update=True,
            )
        )

        assert result == 1
        defaults = fb.get_ensemble_defaults(1, "test-cmd", "gpu")
        assert defaults["input_params"]["trajL"] == "0.5"
        assert defaults["job_params"]["nodes"] == "4"

    def test_wit_input_dry_run_lists_missing_required_params(self, tmp_path, capsys):
        """Input-only commands should also show missing required dry-run rows."""
        from MDWFutils.cli.commands.wit_input import WitInputCommand

        fb = FakeBackend({1: self._make_ensemble(tmp_path)})
        cmd = WitInputCommand(fb)

        result = cmd.execute(_command_args(dry_run=True))

        assert result == 0
        out = capsys.readouterr()
        for name in ("Configurations.first", "Configurations.last"):
            line = _param_line(out.out, name)
            assert "<required>" in line
            assert "Missing required" in line
# ---------------------------------------------------------------------------
# default_params import subcommand
# ---------------------------------------------------------------------------


class TestDefaultParamsImport:
    def test_import_yaml_stores_defaults(self, tmp_path):
        """YAML import should parse and store defaults."""
        ens = make_ensemble(tmp_path)
        fb = FakeBackend({1: ens})

        # Create YAML file
        yaml_path = tmp_path / "mdwf_default_params.yaml"
        yaml_path.write_text(
            "HMC:\n  input_params: trajL=0.5 n_trajec=10 lvl_sizes=9,1,1\n  job_params: nodes=4 time_limit=04:00:00\n"
        )

        from MDWFutils.cli.commands.default_params import _import_yaml
        result = _import_yaml(fb, 1, ens, "hmc-script", "gpu")
        assert result == 0

        defaults = fb.get_ensemble_defaults(1, "hmc-script", "gpu")
        assert defaults["input_params"]["trajL"] == "0.5"
        assert defaults["job_params"]["nodes"] == "4"

    def test_import_yaml_dict_params(self, tmp_path):
        """YAML import should handle dict-style params."""
        ens = make_ensemble(tmp_path)
        fb = FakeBackend({1: ens})

        yaml_path = tmp_path / "mdwf_default_params.yaml"
        yaml_path.write_text(
            "HMC:\n  input_params:\n    trajL: 0.5\n    n_trajec: 10\n  job_params:\n    nodes: 4\n"
        )

        from MDWFutils.cli.commands.default_params import _import_yaml
        result = _import_yaml(fb, 1, ens, "hmc-script", "gpu")
        assert result == 0

        defaults = fb.get_ensemble_defaults(1, "hmc-script", "gpu")
        assert defaults["input_params"]["trajL"] == "0.5"
        assert defaults["job_params"]["nodes"] == "4"

    def test_import_yaml_missing_file(self, tmp_path, capfd):
        """YAML import should error when file is missing."""
        ens = make_ensemble(tmp_path)
        fb = FakeBackend({1: ens})

        from MDWFutils.cli.commands.default_params import _import_yaml
        result = _import_yaml(fb, 1, ens, "hmc-script", "gpu")
        assert result == 1
        out = capfd.readouterr()
        assert "ERROR" in out.err
