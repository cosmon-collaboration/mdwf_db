from ..operations import BUILD_QDPXX
from ..schema import common_build_params
from ...jobs.schema import ContextParam
from ._gpu_deps import GpuPackageBuilder


class QdpxxGpuBuilder(GpuPackageBuilder):
    type_name = "qdpxx_gpu"
    template_name = "build/qdpxx_gpu.j2"
    operation = BUILD_QDPXX
    package = "qdpxx"
    git_url = "https://github.com/usqcd-software/qdpxx"
    git_branch = "devel"

    build_params_schema = [
        *common_build_params(),
        ContextParam("git_url", str, default="https://github.com/usqcd-software/qdpxx"),
        ContextParam("git_branch", str, default="devel"),
    ]

    def _build_context(self, backend, ensemble_id, ensemble, site, build_params, *, command_line=""):
        return self._package_context(ensemble_id, site, build_params, command_line=command_line, pkg="qdpxx")
