"""Site profile resolution for build scripts."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from typing import Dict, Optional

DEFAULT_SOFTWARE_ROOT = "/global/cfs/cdirs/m2986/cosmon/mdwf/software"
DEFAULT_GPU_ARCH = "sm_80"
DEFAULT_CRAY_PE_CC = "/opt/cray/pe/craype/2.7.30/bin/cc"
DEFAULT_PERM_GROUP = "m2986"


@dataclass(frozen=True)
class SiteProfile:
    """Resolved Perlmutter software paths and toolchain defaults."""

    base: str
    source_dir: str
    build_dir: str
    install_gpu_dir: str
    install_cpu_dir: str
    scripts_dir: str
    perm_fix: str
    perm_group: str
    gmp_prefix: str
    mpfr_prefix: str
    gpu_arch: str
    cray_pe_cc: str

    def as_dict(self) -> Dict[str, str]:
        return {f.name: getattr(self, f.name) for f in fields(self)}


def resolve_site_profile(overrides: Optional[Dict[str, str]] = None) -> SiteProfile:
    """Resolve site profile from env, defaults, and CLI overrides."""
    params = overrides.copy() if overrides else {}
    base = params.pop("base", None) or os.getenv("MDWF_SOFTWARE_ROOT", DEFAULT_SOFTWARE_ROOT)
    base = str(base).rstrip("/")

    install_cpu = params.pop("install_cpu_dir", f"{base}/install")
    profile = SiteProfile(
        base=base,
        source_dir=params.pop("source_dir", f"{base}/source"),
        build_dir=params.pop("build_dir", f"{base}/build"),
        install_gpu_dir=params.pop("install_gpu_dir", f"{base}/install_gpu"),
        install_cpu_dir=install_cpu,
        scripts_dir=params.pop("scripts_dir", f"{base}/scripts"),
        perm_fix=params.pop("perm_fix", f"{base}/scripts/perm_fix_m2986.sh"),
        perm_group=params.pop("perm_group", DEFAULT_PERM_GROUP),
        gmp_prefix=params.pop("gmp_prefix", f"{install_cpu}/gmp"),
        mpfr_prefix=params.pop("mpfr_prefix", f"{install_cpu}/mpfr"),
        gpu_arch=params.pop("gpu_arch", DEFAULT_GPU_ARCH),
        cray_pe_cc=params.pop("cray_pe_cc", DEFAULT_CRAY_PE_CC),
    )

    if params:
        known = {f.name for f in fields(SiteProfile)}
        unknown = set(params) - known
        if unknown:
            raise ValueError(f"Unknown site profile overrides: {sorted(unknown)}")

    return profile
