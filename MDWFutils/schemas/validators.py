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


class EnsembleCreate(BaseModel):
    """Validate ensemble creation payloads."""

    directory: str
    physics: PhysicsParams
    status: str = Field(pattern="^(TUNING|PRODUCTION)$")
    description: Optional[str] = None
    nickname: Optional[str] = None


