"""Shared helpers for GPU package build builders."""

from __future__ import annotations

from typing import Dict

from ..schema import BuildContextBuilder, common_build_params
from ..site import SiteProfile


class GpuPackageBuilder(BuildContextBuilder):
    """Base for autotools/cmake GPU dependency builds."""

    git_url: str = ""
    git_branch: str = ""

    build_params_schema = [
        *common_build_params(),
        # subclasses may extend
    ]

    def _package_context(
        self,
        ensemble_id: int,
        site: SiteProfile,
        build_params: Dict,
        *,
        command_line: str,
        pkg: str,
        install_subdir: str | None = None,
    ) -> Dict:
        install_root = site.install_gpu_dir
        install_prefix = f"{install_root}/{install_subdir or pkg}"
        prefix = f"build_{pkg}_gpu"
        ctx = self.base_context(
            ensemble_id,
            site,
            build_params,
            command_line=command_line,
            install_prefix=install_prefix,
        )
        ctx.update(
            {
                "pkg": pkg,
                "git_url": build_params.get("git_url", self.git_url),
                "git_branch": build_params.get("git_branch", self.git_branch),
                "build_script_path": f"{site.scripts_dir}/{prefix}.sh",
            }
        )
        ctx.update(self.script_output(site, prefix))
        return ctx
