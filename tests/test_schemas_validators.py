"""Tests for Pydantic schema validators."""

import pytest
from pydantic import ValidationError as PydanticValidationError

from MDWFutils.schemas.validators import EnsembleCreate, GridBuildParams, PhysicsParams


def test_physics_params_valid():
    p = PhysicsParams(beta=4.0, b=1.75, Ls=10, mc=0.85, ms=0.07, ml=0.02, L=32, T=64)
    assert p.L == 32


def test_physics_params_rejects_nonpositive():
    with pytest.raises(PydanticValidationError):
        PhysicsParams(beta=0, b=1.75, Ls=10, mc=0.85, ms=0.07, ml=0.02, L=32, T=64)


def test_ensemble_create_status_pattern():
    doc = EnsembleCreate(directory="/tmp/e", physics=PhysicsParams(beta=4.0, b=1.75, Ls=10, mc=0.85, ms=0.07, ml=0.02, L=32, T=64), status="TUNING")
    assert doc.status == "TUNING"

    with pytest.raises(PydanticValidationError):
        EnsembleCreate(directory="/tmp/e", physics=PhysicsParams(beta=4.0, b=1.75, Ls=10, mc=0.85, ms=0.07, ml=0.02, L=32, T=64), status="INVALID")

def test_grid_build_params_accepts_positive_hasenbusch():
    params = GridBuildParams(
        beta_line="default",
        light_mass=0.003,
        hasenbusch=[0.5, 1.0],
        nlvl1=1,
        eofa_integrator_level=1,
    )
    assert params.hasenbusch == [0.5, 1.0]


def test_grid_build_params_rejects_empty_hasenbusch():
    with pytest.raises(PydanticValidationError):
        GridBuildParams(
            beta_line="default",
            light_mass=0.003,
            hasenbusch=[],
            nlvl1=1,
            eofa_integrator_level=1,
        )


def test_grid_build_params_rejects_nonpositive_mass():
    with pytest.raises(PydanticValidationError):
        GridBuildParams(
            beta_line="default",
            light_mass=0.003,
            hasenbusch=[0.5, -1.0],
            nlvl1=1,
            eofa_integrator_level=1,
        )
