import tempfile
import unittest
from pathlib import Path

from MDWFutils.remote.commands import build_remote_command, get_remote_command_specs
from MDWFutils.remote.profiles import RemoteProfile, load_remote_profile
from MDWFutils.remote.transport import run_remote_command


class RemoteProfileTests(unittest.TestCase):
    def test_missing_profile_in_config_is_clear_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "remote.yaml"
            config.write_text(
                "profiles:\n"
                "  pm:\n"
                "    host: perlmutter\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "Remote profile 'nersc' not found"):
                load_remote_profile("nersc", config)

    def test_zero_config_falls_back_to_ssh_host_alias(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing.yaml"

            profile = load_remote_profile("perlmutter", missing)

        self.assertEqual(profile.host, "perlmutter")
        self.assertEqual(profile.remote_mdwf_db, "mdwf_db")


class RemoteCommandTests(unittest.TestCase):
    def setUp(self):
        self.profile = RemoteProfile(name="pm", host="perlmutter")

    def test_known_template_builds_safe_monitor_command(self):
        argv = build_remote_command(
            "monitor",
            {"ensemble": "1", "dry_run": True},
            self.profile,
        )

        self.assertEqual(argv, ["mdwf_db", "monitor", "--json", "--ensemble", "1", "--dry-run", "--source", "auto"])

    def test_unknown_template_argument_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported remote argument"):
            build_remote_command("doctor", {"shell": "rm -rf /"}, self.profile)

    def test_remote_run_rejects_non_allowlisted_mdwf_subcommand(self):
        with self.assertRaisesRegex(ValueError, "remote run only allows"):
            build_remote_command("run", {"argv": ["mdwf_db", "remove-ensemble", "-e", "1"]}, self.profile)

    def test_remote_run_rejects_mutating_submit_without_dry_run(self):
        with self.assertRaisesRegex(ValueError, "remote run only allows"):
            build_remote_command(
                "run",
                {"argv": ["mdwf_db", "submit", "-e", "1", "-o", "mres", "--script", "job.slurm"]},
                self.profile,
            )

    def test_remote_run_allows_dry_run_submit_and_adds_json(self):
        argv = build_remote_command(
            "run",
            {"argv": ["mdwf_db", "submit", "-e", "1", "-o", "mres", "--script", "job.slurm", "--dry-run"]},
            self.profile,
        )

        self.assertIn("--dry-run", argv)
        self.assertIn("--json", argv)


class RemoteTransportTests(unittest.TestCase):
    def test_dry_run_transport_returns_ssh_argv_without_connecting(self):
        profile = RemoteProfile(
            name="pm",
            host="perlmutter",
            workdir="/global/cfs/cdirs/proj/user/mdwf_db",
            python_env_setup="module load python",
        )
        spec = get_remote_command_specs()["doctor"]

        result = run_remote_command(
            profile,
            spec,
            ["mdwf_db", "perlmutter", "doctor", "--json"],
            dry_run_transport=True,
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.argv[:3], ["ssh", "perlmutter", "bash"])
        self.assertIn("cd /global/cfs/cdirs/proj/user/mdwf_db", result.argv[-1])
        self.assertIn("module load python", result.argv[-1])
        self.assertIn("mdwf_db perlmutter doctor --json", result.argv[-1])


if __name__ == "__main__":
    unittest.main()
