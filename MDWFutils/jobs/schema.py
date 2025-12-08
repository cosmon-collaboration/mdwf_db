"""Context parameter schema definitions for job builders."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..backends.base import DatabaseBackend
from ..exceptions import ValidationError

# Common default paths used across job types
DEFAULT_CONDA_ENV = "/global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf"
DEFAULT_WIT_ENV = "source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh"
DEFAULT_WIT_BIND = "/global/cfs/cdirs/m2986/cosmon/mdwf/ANALYSIS/WIT/bind.sh"
DEFAULT_OGEOM = "1,1,1,4"
DEFAULT_GLU_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install/GLU_ICC/bin/GLU"
DEFAULT_CONFIG_PREFIX = "ckpoint_EODWF_lat."


@dataclass
class ContextParam:
    """Declarative parameter definition for context builders.
    
    Similar to ParamDef but used by context builders to define their own
    parameter schemas with defaults, types, and validation rules.
    """

    name: str
    type: type
    required: bool = False
    default: Optional[Any] = None
    help: str = ""
    choices: Optional[List[Any]] = None


def _deduplicate_schema(schema: Optional[List[ContextParam]]) -> List[ContextParam]:
    """Deduplicate schema params, keeping the LAST occurrence of each name.
    
    This allows subclasses to override common params by redefining them
    after spreading *common_slurm_params() or *common_wit_gpu_params().
    """
    if not schema:
        return []
    
    # Keep last occurrence
    seen = set()
    result = []
    for param in reversed(schema):
        if param.name not in seen:
            result.append(param)
            seen.add(param.name)
    
    return list(reversed(result))


def common_slurm_params() -> List[ContextParam]:
    """Common SLURM parameters with sensible defaults.
    
    Builders can override specific params (like constraint) by redefining
    them after the spread. The last definition wins.
    """
    return [
        ContextParam("account", str, default="m2986", help="SLURM account"),
        ContextParam("queue", str, default="regular", help="SLURM queue/partition"),
        ContextParam("time_limit", str, default="01:00:00", help="SLURM time limit"),
        ContextParam("nodes", int, default=1, help="Number of nodes"),
        ContextParam("cpus_per_task", int, default=256, help="CPUs per task"),
        ContextParam("ranks", int, default=1, help="MPI ranks"),
        ContextParam("mail_user", str, help="User email for notifications"),
        ContextParam("mail_type", str, default="ALL", help="Mail notification types"),
        ContextParam("job_name", str, help="Job name (computed if not provided)"),
    ]


def common_wit_gpu_params() -> List[ContextParam]:
    """Common parameters for WIT GPU jobs.
    
    Includes common SLURM params plus WIT GPU-specific defaults.
    Builders can override specific params by redefining them after the spread.
    """
    return [
        *common_slurm_params(),
        ContextParam("account", str, default="m2986_g", help="SLURM account"),
        ContextParam("constraint", str, default="gpu", help="Node constraint"),
        ContextParam("time_limit", str, default="06:00:00", help="SLURM time limit"),
        ContextParam("ranks", int, default=4, help="MPI ranks"),
        ContextParam("cpus_per_task", int, default=32, help="CPUs per task"),
        ContextParam("gpus", int, default=4, help="GPUs per node"),
        ContextParam("gpu_bind", str, default="none", help="GPU binding policy"),
        ContextParam("run_dir", str, help="Working directory (defaults to ensemble directory)"),
        ContextParam("conda_env", str, default=DEFAULT_CONDA_ENV, help="Conda environment path"),
        ContextParam("bind_script", str, default=DEFAULT_WIT_BIND, help="CPU binding script"),
        ContextParam("ogeom", str, default=DEFAULT_OGEOM, help="Geometry override"),
    ]


class ContextBuilder(ABC):
    """Abstract base class for all context builders (job and input).
    
    Uses template method pattern:
    - Base class handles common setup (ensemble loading, path resolution)
    - Subclasses implement _build_context() for specific logic
    - Schemas are class attributes (job_params_schema, input_params_schema)
    """
    
    # Subclasses should override these
    job_params_schema: Optional[List[ContextParam]] = None
    input_params_schema: Optional[List[ContextParam]] = None
    
    def build(self, backend: DatabaseBackend, ensemble_id: int, 
              job_params: Dict = None, input_params: Dict = None) -> Dict:
        """Template method - auto-merges schema params with computed values.
        
        Args:
            backend: Database backend instance
            ensemble_id: Ensemble identifier
            job_params: Job parameters dict (None for input-only builders)
            input_params: Input parameters dict (None if not provided)
        
        Returns:
            Context dictionary for template rendering
        """
        # Normalize: if job_params is a dict without typical job keys, 
        # it's really input_params (input builders called with positional arg)
        if job_params is not None and input_params is None:
            # Check if this looks like input params (no typical job params)
            if not any(k in job_params for k in ["config_start", "n_trajec", "nodes", "time_limit"]):
                input_params = job_params
                job_params = {}
        
        # Common setup
        ensemble = self._get_ensemble(backend, ensemble_id)
        physics = self._get_physics(ensemble)
        
        # Deduplicate schemas (last definition wins for overrides)
        job_schema_deduped = _deduplicate_schema(self.job_params_schema)
        input_schema_deduped = _deduplicate_schema(self.input_params_schema)

        # Apply schema defaults + validation to job/input params
        job_values = self._apply_schema(job_params or {}, job_schema_deduped, param_type="job", cast_to_str=False)
        input_values = self._apply_schema(input_params or {}, input_schema_deduped, param_type="input", cast_to_str=True)
        
        # Let subclass build computed/special values
        computed_context = self._build_context(
            backend, ensemble_id, ensemble, physics, 
            job_params or {}, input_params or {}
        )
        
        # Merge with precedence: schema+defaults, then computed (computed wins)
        final_context = {}
        final_context.update(job_values)
        final_context.update(input_values)
        final_context.update(computed_context)
        
        return final_context
    
    def _deduplicate_schema(self, schema: Optional[List[ContextParam]]) -> List[ContextParam]:
        """Deduplicate schema params, keeping the LAST occurrence of each name.
        
        This allows subclasses to override common params by redefining them
        after spreading *common_slurm_params().
        """
        if not schema:
            return []
        
        # Build dict to keep last occurrence
        param_dict = {}
        for param in schema:
            param_dict[param.name] = param
        
        # Return in original order (excluding earlier duplicates)
        seen = set()
        result = []
        for param in reversed(schema):
            if param.name not in seen:
                result.append(param)
                seen.add(param.name)
        
        return list(reversed(result))

    def _apply_schema(self, params: Dict, schema: List[ContextParam], *, param_type: str, cast_to_str: bool) -> Dict:
        """Apply defaults, validate required/choices, and type-cast."""
        typed: Dict[str, Any] = {}
        errors: List[str] = []
        missing: List[ContextParam] = []

        for definition in schema:
            has_value = definition.name in params and params[definition.name] is not None
            if has_value:
                value = params[definition.name]
            elif definition.default is not None:
                value = definition.default
            else:
                if definition.required:
                    missing.append(definition)
                continue

            # Type cast
            try:
                cast_value = definition.type(value)
            except (TypeError, ValueError):
                errors.append(f"{definition.name}: expected {definition.type.__name__}")
                continue

            # Choices check
            if definition.choices and cast_value not in definition.choices:
                errors.append(f"{definition.name} must be one of: {', '.join(map(str, definition.choices))}")
                continue

            typed[definition.name] = str(cast_value) if cast_to_str else cast_value

        if missing:
            flag = "-i" if param_type == "input" else "-j"
            msg = f"\nMissing required {param_type} parameters (pass with {flag}):\n"
            for param in missing:
                msg += f"  • {param.name}: {param.help}\n"
            examples = " ".join(f"{p.name}=<value>" for p in missing)
            msg += f"\nExample: {flag} \"{examples}\""
            errors.append(msg)

        if errors:
            raise ValidationError("\n".join(errors))

        return typed

    def _get_ensemble(self, backend: DatabaseBackend, ensemble_id: int) -> Dict:
        """Get ensemble document."""
        from ..exceptions import EnsembleNotFoundError
        ensemble = backend.get_ensemble(ensemble_id)
        if not ensemble:
            raise EnsembleNotFoundError(ensemble_id)
        return ensemble
    
    def _get_physics(self, ensemble: Dict) -> Dict:
        """Extract physics parameters from ensemble."""
        return ensemble.get("physics", {})
    
    def _resolve_run_dir(self, ensemble: Dict, job_params: Dict) -> Path:
        """Get run directory from params or ensemble directory."""
        ensemble_dir = Path(ensemble["directory"]).resolve()
        run_dir = job_params.get("run_dir")
        return Path(run_dir).resolve() if run_dir else ensemble_dir


class WitGPUContextBuilder(ContextBuilder):
    """Base class for WIT GPU job builders with shared setup logic.
    
    Provides common helper methods for directory setup and geometry parsing
    that are shared across all WIT GPU job types (mres, mres_mq, meson2pt, zv).
    """
    
    def _setup_wit_workdir(self, ensemble: Dict, job_params: Dict, subdir: str) -> Tuple[Path, Path]:
        """Create WIT working directory structure.
        
        Args:
            ensemble: Ensemble document dict
            job_params: Job parameters dict
            subdir: Subdirectory name (e.g., "mres", "meson2pt")
        
        Returns:
            Tuple of (workdir, log_dir) Path objects
        """
        work_root = self._resolve_run_dir(ensemble, job_params)
        workdir = work_root / subdir
        log_dir = workdir / "jlog"
        workdir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        (workdir / "slurm").mkdir(parents=True, exist_ok=True)
        return workdir, log_dir
    
    def _parse_geometry(self, physics: Dict, job_params: Dict) -> Tuple[int, int, List[int], List[int]]:
        """Parse and validate geometry parameters.
        
        Args:
            physics: Physics parameters dict (from ensemble["physics"])
            job_params: Job parameters dict
        
        Returns:
            Tuple of (L, T, ogeom, lgeom)
        """
        from .utils import parse_ogeom, validate_geometry
        
        L = int(physics["L"])
        T = int(physics["T"])
        ogeom_str = job_params.get("ogeom") or DEFAULT_OGEOM
        ogeom = parse_ogeom(str(ogeom_str))
        lgeom = validate_geometry(L, T, ogeom)
        return L, T, ogeom, lgeom
    
    @abstractmethod
    def _build_context(self, backend: DatabaseBackend, ensemble_id: int,
                      ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Subclasses implement this to build their specific context.
        
        Args:
            backend: Database backend instance
            ensemble_id: Ensemble identifier
            ensemble: Ensemble document dict
            physics: Physics parameters dict (from ensemble["physics"])
            job_params: Job parameters dict (empty for input-only builders)
            input_params: Input parameters dict
        
        Returns:
            Context dictionary for template rendering
        """
        pass
    
    def _get_ensemble(self, backend: DatabaseBackend, ensemble_id: int) -> Dict:
        """Get ensemble document."""
        from ..exceptions import EnsembleNotFoundError
        ensemble = backend.get_ensemble(ensemble_id)
        if not ensemble:
            raise EnsembleNotFoundError(ensemble_id)
        return ensemble
    
    def _get_physics(self, ensemble: Dict) -> Dict:
        """Extract physics parameters from ensemble."""
        return ensemble.get("physics", {})
    
    def _resolve_run_dir(self, ensemble: Dict, job_params: Dict) -> Path:
        """Get run directory from params or ensemble directory.
        
        Args:
            ensemble: Ensemble document dict
            job_params: Job parameters dict
        
        Returns:
            Resolved Path to run directory
        """
        ensemble_dir = Path(ensemble["directory"]).resolve()
        run_dir = job_params.get("run_dir")
        return Path(run_dir).resolve() if run_dir else ensemble_dir

