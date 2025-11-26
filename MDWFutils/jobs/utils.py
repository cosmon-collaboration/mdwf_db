"""Shared helpers used by job context builders and legacy scripts."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable, List, Sequence

from MDWFutils.backends import get_backend
from MDWFutils.backends.base import DatabaseBackend
from MDWFutils.exceptions import ValidationError


@lru_cache(maxsize=None)
def _cached_backend(connection_string: str) -> DatabaseBackend:
    """Return a cached backend instance for legacy callers."""
    return get_backend(connection_string)


def _resolve_backend(source) -> DatabaseBackend:
    """Accept either a backend instance or a connection string."""
    if isinstance(source, DatabaseBackend):
        return source
    if isinstance(source, str):
        return _cached_backend(source)
    raise TypeError("Expected DatabaseBackend instance or connection string")


def get_ensemble_doc(source, ensemble_id: int) -> dict:
    """
    Fetch the ensemble document given either a backend instance or connection string.

    This maintains backwards compatibility with older job helpers while allowing
    the new context builders to pass the already-instantiated backend.
    """
    backend = _resolve_backend(source)
    doc = backend.get_ensemble(ensemble_id)
    if not doc:
        raise RuntimeError(f"Ensemble {ensemble_id} not found")
    return doc


def get_physics_params(doc: dict) -> dict:
    """Return a shallow copy of the physics parameters."""
    return dict(doc.get("physics", {}))


def parse_ogeom(value: str) -> List[int]:
    """Parse an ogeom string like '1,1,1,2' into a list of ints."""
    try:
        parts = [int(part.strip()) for part in value.split(",")]
    except Exception as exc:
        raise ValidationError(f"Invalid ogeom '{value}'") from exc

    if len(parts) != 4:
        raise ValidationError(f"ogeom must have 4 components, got {len(parts)}: {value}")
    if any(part <= 0 for part in parts):
        raise ValidationError(f"ogeom components must be positive: {value}")
    return parts


def validate_geometry(L: int, T: int, ogeom: Sequence[int]) -> List[int]:
    """
    Validate that ogeom tiles the lattice evenly and return the derived lgeom values.

    Each component of lgeom must be even to satisfy domain wall fermion job constraints.
    """
    if len(ogeom) != 4:
        raise ValidationError(f"ogeom must have 4 components, got {len(ogeom)}")

    try:
        lgeom = [L // ogeom[i] for i in range(3)] + [T // ogeom[3]]
    except Exception as exc:
        raise ValidationError(f"Invalid ogeom {ogeom} for lattice {L}x{T}") from exc

    # Ensure perfect divisibility
    if any(lgeom[i] * ogeom[i] != L for i in range(3)) or lgeom[3] * ogeom[3] != T:
        raise ValidationError(f"ogeom {ogeom} does not evenly tile lattice {L}x{L}x{L}x{T}")

    if any(dim % 2 != 0 for dim in lgeom):
        raise ValidationError(f"lgeom values must be even, got {lgeom}")

    return lgeom


def compute_kappa(mass: float) -> float:
    """Compute kappa from a quark mass: kappa = 1 / (2m + 8)."""
    if mass <= 0:
        raise ValidationError(f"Mass must be positive to compute kappa, got {mass}")
    return 1.0 / (2.0 * mass + 8.0)


def ensure_positive(name: str, value: float) -> float:
    """Ensure value is positive, raising ValidationError otherwise."""
    if value <= 0:
        raise ValidationError(f"{name} must be positive, got {value}")
    return value


def ensure_keys(mapping: dict, keys: Iterable[str]):
    """Ensure mapping contains all keys, raising ValidationError otherwise."""
    missing = [key for key in keys if key not in mapping]
    if missing:
        raise ValidationError(f"Missing required parameters: {', '.join(missing)}")


def require_all(params: Dict, requirements: Dict[str, str], param_type: str = "job") -> Dict:
    """Check for multiple required parameters at once.
    
    Args:
        params: Parameter dictionary
        requirements: Dict mapping parameter names to descriptions
        param_type: "job" or "input" for error message context
        
    Returns:
        Dict of validated non-empty values
        
    Raises:
        ValidationError listing all missing parameters with examples
    """
    missing = []
    for key, description in requirements.items():
        if params.get(key) in (None, ""):
            missing.append((key, description))
    
    if missing:
        flag = "-i" if param_type == "input" else "-j"
        msg = f"\nMissing required {param_type} parameters (pass with {flag}):\n"
        for key, desc in missing:
            msg += f"  • {key}: {desc}\n"
        examples = " ".join(f"{k}=<value>" for k, _ in missing)
        msg += f"\nExample: {flag} \"{examples}\""
        raise ValidationError(msg)
    
    return {key: params[key] for key in requirements.keys()}
