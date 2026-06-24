"""Pydantic models for validating database documents."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator


class PhysicsParams(BaseModel):
    """Validate ensemble physics parameters."""

    beta: float = Field(gt=0, description="Gauge coupling")
    b: float = Field(gt=0, description="Domain wall height")
    Ls: int = Field(gt=0, description="Fifth dimension extent")
    mc: float = Field(gt=0, description="Charm quark mass")
    ms: float = Field(gt=0, description="Strange quark mass")
    ml: float = Field(gt=0, description="Light quark mass")
    L: int = Field(gt=0, description="Spatial lattice size")
    T: int = Field(gt=0, description="Temporal lattice size")


class GridBuildParams(BaseModel):
    """Validate grid_build parameters for Nf2p1p1 generation."""

    beta_line: str
    light_mass: float = Field(gt=0)
    hasenbusch: list[float]
    nlvl1: int = Field(ge=0)
    eofa_integrator_level: int = Field(ge=1, le=2)
    charm_mass_factor: float = Field(default=11.8, gt=0)
    pv_mass: float = Field(default=1.0, gt=0)
    eofa_hs_extra: Optional[float] = None
    notes: Optional[str] = None

    @field_validator("hasenbusch")
    @classmethod
    def _positive_masses(cls, value):
        if not value or any(m <= 0 for m in value):
            raise ValueError("hasenbusch must be a non-empty list of positive masses")
        return value


class EnsembleCreate(BaseModel):
    """Validate ensemble creation payloads."""

    directory: str
    physics: PhysicsParams
    status: str = Field(pattern="^(TUNING|PRODUCTION)$")
    description: Optional[str] = None
    nickname: Optional[str] = None


