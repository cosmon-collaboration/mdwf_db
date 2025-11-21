"""Pydantic models for validating database documents."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, validator


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

    @validator("Ls")
    def ls_must_be_even(cls, value: int) -> int:
        if value % 2 != 0:
            raise ValueError("Ls should typically be even")
        return value

    @validator("T")
    def t_must_be_greater_than_l(cls, value: int, values: dict) -> int:
        lattice_size = values.get("L")
        if lattice_size is not None and value < lattice_size:
            raise ValueError("T should typically be >= L")
        return value


class EnsembleCreate(BaseModel):
    """Validate ensemble creation payloads."""

    directory: str
    physics: PhysicsParams
    status: str = Field(pattern="^(TUNING|PRODUCTION)$")
    description: Optional[str] = None
    nickname: Optional[str] = None


