from ..operations import BUILD_GLU
from ..schema import BuildContextBuilder, common_build_params


class GluCpuBuilder(BuildContextBuilder):
    type_name = "glu_cpu"
    template_name = "build/glu_cpu.j2"
    operation = BUILD_GLU
    package = "GLU"
    build_params_schema = common_build_params()

    def _build_context(self, backend, ensemble_id, ensemble, site, build_params, *, command_line=""):
        install_prefix = f"{site.install_cpu_dir}/GLU_ICC"
        ctx = self.base_context(
            ensemble_id,
            site,
            build_params,
            command_line=command_line,
            install_prefix=install_prefix,
        )
        prefix = "build_glu"
        ctx.update({"build_script_path": f"{site.scripts_dir}/{prefix}.sh"})
        ctx.update(self.script_output(site, prefix))
        return ctx
