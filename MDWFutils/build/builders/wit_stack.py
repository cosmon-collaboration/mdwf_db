from ..operations import BUILD_WIT_STACK
from ..schema import BuildContextBuilder, common_build_params


class WitStackBuilder(BuildContextBuilder):
    type_name = "wit_stack"
    template_name = "build/wit_stack.j2"
    operation = BUILD_WIT_STACK
    package = "wit-stack"
    build_params_schema = common_build_params()

    def _build_context(self, backend, ensemble_id, ensemble, site, build_params, *, command_line=""):
        ctx = self.base_context(
            ensemble_id,
            site,
            build_params,
            command_line=command_line,
            install_prefix=site.install_gpu_dir,
        )
        prefix = "build_wit_stack"
        ctx.update(
            {
                "build_script_path": f"{site.scripts_dir}/{prefix}.sh",
            }
        )
        ctx.update(self.script_output(site, prefix))
        return ctx
