from ..operations import BUILD_QUDA
from ..schema import common_build_params
from ...jobs.schema import ContextParam
from ._gpu_deps import GpuPackageBuilder


class QudaGpuBuilder(GpuPackageBuilder):
    type_name = "quda_gpu"
    template_name = "build/quda_gpu.j2"
    operation = BUILD_QUDA
    package = "quda"
    git_url = "git@github.com:lattice/quda.git"
    git_branch = "develop"

    build_params_schema = [
        *common_build_params(),
        ContextParam("git_url", str, default="git@github.com:lattice/quda.git"),
        ContextParam("git_branch", str, default="develop"),
    ]

    def _build_context(self, backend, ensemble_id, ensemble, site, build_params, *, command_line=""):
        return self._package_context(ensemble_id, site, build_params, command_line=command_line, pkg="quda")
