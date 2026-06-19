from ..operations import BUILD_LIBXML2
from ..schema import common_build_params
from ...jobs.schema import ContextParam
from ._gpu_deps import GpuPackageBuilder


class Libxml2GpuBuilder(GpuPackageBuilder):
    type_name = "libxml2_gpu"
    template_name = "build/libxml2_gpu.j2"
    operation = BUILD_LIBXML2
    package = "libxml2"
    git_url = "https://github.com/GNOME/libxml2.git"
    git_branch = "v2.9.14"

    build_params_schema = [
        *common_build_params(),
        ContextParam("git_url", str, default="https://github.com/GNOME/libxml2.git"),
        ContextParam("git_branch", str, default="v2.9.14"),
    ]

    def _build_context(self, backend, ensemble_id, ensemble, site, build_params, *, command_line=""):
        return self._package_context(
            ensemble_id, site, build_params, command_line=command_line, pkg="libxml2"
        )
