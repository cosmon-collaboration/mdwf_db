from pathlib import Path

from ..action import format_action, nf2p1p1_output_path
from ..operations import BUILD_GRID_GPU, BUILD_GRID_CPU
from ..schema import BuildContextBuilder, common_build_params, provenance_header
from ..validate_grid_build import merge_physics_and_grid_build, validate_grid_build
from ...exceptions import ValidationError
from ...jobs.schema import ContextParam


class GridCcBuilder(BuildContextBuilder):
    type_name = "grid_cc"
    template_name = "build/grid/Nf2p1p1.cc.j2"
    operation = ""
    package = "grid-hmc"
    build_params_schema = [
        *common_build_params(),
        ContextParam("force_physics_mismatch", bool, default=False),
    ]

    def _build_context(self, backend, ensemble_id, ensemble, site, build_params, *, command_line=""):
        physics = ensemble.get("physics", {})
        grid_build = ensemble.get("grid_build", {})
        validate_grid_build(
            physics,
            grid_build,
            force_physics_mismatch=bool(build_params.get("force_physics_mismatch")),
        )
        action = format_action(physics)
        merged = merge_physics_and_grid_build(physics, grid_build)
        merged["action"] = action
        merged["provenance"] = provenance_header(
            command_line or "mdwf_db build grid cc",
            ensemble_id,
            action,
        )
        out_path = nf2p1p1_output_path(site.scripts_dir, action)
        merged.update(
            {
                "ensemble_id": ensemble_id,
                "_output_dir": str(Path(out_path).parent),
                "_output_prefix": f"Nf2p1p1_{action}",
                "_output_suffix": ".cc",
                "_executable": False,
            }
        )
        return merged


class GridHmcGpuBuilder(BuildContextBuilder):
    type_name = "grid_hmc_gpu"
    template_name = "build/grid_hmc_gpu.j2"
    operation = BUILD_GRID_GPU
    package = "grid-hmc-gpu"
    build_params_schema = common_build_params()

    def _build_context(self, backend, ensemble_id, ensemble, site, build_params, *, command_line=""):
        return _grid_hmc_context(
            self, ensemble_id, ensemble, site, build_params, gpu=True, command_line=command_line
        )


class GridHmcCpuBuilder(BuildContextBuilder):
    type_name = "grid_hmc_cpu"
    template_name = "build/grid_hmc_cpu.j2"
    operation = BUILD_GRID_CPU
    package = "grid-hmc-cpu"
    build_params_schema = common_build_params()

    def _build_context(self, backend, ensemble_id, ensemble, site, build_params, *, command_line=""):
        return _grid_hmc_context(
            self, ensemble_id, ensemble, site, build_params, gpu=False, command_line=command_line
        )


def _grid_hmc_context(
    builder: BuildContextBuilder,
    ensemble_id: int,
    ensemble: dict,
    site,
    build_params: dict,
    *,
    gpu: bool,
    command_line: str,
) -> dict:
    from ..action import grid_build_dir_name, grid_hmc_exec_path, grid_install_prefix, nf2p1p1_output_path

    physics = ensemble.get("physics", {})
    if not physics:
        raise ValidationError("Ensemble missing physics parameters")
    action = format_action(physics)
    cc_path = nf2p1p1_output_path(site.scripts_dir, action)
    if not Path(cc_path).is_file():
        raise ValidationError(
            f"Missing {cc_path}; run 'mdwf_db build grid cc -e {ensemble_id}' first"
        )

    install_root = site.install_gpu_dir if gpu else site.install_cpu_dir
    install_prefix = grid_install_prefix(install_root, action)
    grid_dir = grid_build_dir_name(action, gpu=gpu)
    suffix = "gpu" if gpu else "cpu"
    prefix = f"build_grid_{action}_{suffix}"
    ctx = builder.base_context(
        ensemble_id,
        site,
        build_params,
        command_line=command_line,
        install_prefix=install_prefix,
    )
    pkg_root = Path(__file__).resolve().parents[1] / "grid_sources"
    ctx.update(
        {
            "action": action,
            "grid_dir": grid_dir,
            "install_prefix": install_prefix,
            "hmc_exec_path": grid_hmc_exec_path(install_prefix),
            "nf2p1p1_src": cc_path,
            "make_inc_src": str(pkg_root / "Make.grid_hmc.inc"),
            "gpu": gpu,
            "build_script_path": f"{site.scripts_dir}/grid_scripts/{prefix}.sh",
        }
    )
    ctx.update(builder.script_output(site, prefix, scripts_subdir="grid_scripts"))
    return ctx
