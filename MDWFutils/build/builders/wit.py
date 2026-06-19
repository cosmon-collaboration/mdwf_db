from ..operations import BUILD_WIT
from ..schema import common_build_params
from ...jobs.schema import ContextParam
from ._gpu_deps import GpuPackageBuilder


class WitGpuBuilder(GpuPackageBuilder):
    type_name = "wit_gpu"
    template_name = "build/wit_gpu.j2"
    operation = BUILD_WIT
    package = "wit"
    git_url = "git@github.com:MainzLattice/wit.git"
    git_branch = "main"

    build_params_schema = [
        *common_build_params(),
        ContextParam("git_url", str, default="git@github.com:MainzLattice/wit.git"),
        ContextParam("git_branch", str, default="main"),
    ]

    def _build_context(self, backend, ensemble_id, ensemble, site, build_params, *, command_line=""):
        return self._package_context(ensemble_id, site, build_params, command_line=command_line, pkg="wit")
