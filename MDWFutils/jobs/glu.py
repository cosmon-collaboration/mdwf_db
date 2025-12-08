"""Context builder for GLU smearing input files."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from .schema import ContextBuilder, ContextParam
from .utils import ensure_keys


class GluContextBuilder(ContextBuilder):
    """GLU smearing input file context builder."""
    
    input_params_schema = [
        # Core smearing params (used by smear jobs)
        ContextParam("SMEARTYPE", str, default="STOUT", choices=["STOUT", "APE", "HYP"], help="Smearing algorithm"),
        ContextParam("SMITERS", int, default=8, help="Smearing iterations"),
        ContextParam("ALPHA1", float, default=0.75, help="Alpha1 smearing parameter"),
        ContextParam("ALPHA2", float, default=0.4, help="Alpha2 smearing parameter"),
        ContextParam("ALPHA3", float, default=0.2, help="Alpha3 smearing parameter"),
        
        # GLU-specific params
        ContextParam("MODE", str, default="SMEARING", help="GLU operation mode"),
        ContextParam("CONFNO", str, default="24", help="Configuration number"),
        ContextParam("RANDOM_TRANSFORM", str, default="NO", help="Apply random gauge transform"),
        ContextParam("SEED", str, default="0", help="Random seed"),
        ContextParam("CUTTYPE", str, default="GLUON_PROPS", help="Cut type"),
        ContextParam("HEADER", str, default="NERSC", help="Header type"),
        ContextParam("DIM_0", str, help="Lattice dimension 0 (auto from ensemble)"),
        ContextParam("DIM_1", str, help="Lattice dimension 1 (auto from ensemble)"),
        ContextParam("DIM_2", str, help="Lattice dimension 2 (auto from ensemble)"),
        ContextParam("DIM_3", str, help="Lattice dimension 3 (auto from ensemble)"),
        ContextParam("GFTYPE", str, default="COULOMB", help="Gauge fixing type"),
        ContextParam("GF_TUNE", str, default="0.09", help="Gauge fixing tune parameter"),
        ContextParam("ACCURACY", str, default="14", help="Accuracy parameter"),
        ContextParam("MAX_ITERS", str, default="650", help="Maximum iterations"),
        ContextParam("FIELD_DEFINITION", str, default="LINEAR", help="Field definition"),
        ContextParam("MOM_CUT", str, default="CYLINDER_CUT", help="Momentum cut type"),
        ContextParam("MAX_T", str, default="7", help="Maximum T"),
        ContextParam("MAXMOM", str, default="4", help="Maximum momentum"),
        ContextParam("CYL_WIDTH", str, default="2.0", help="Cylinder width"),
        ContextParam("ANGLE", str, default="60", help="Angle"),
        ContextParam("OUTPUT", str, default="./", help="Output directory"),
        ContextParam("DIRECTION", str, default="ALL", help="Direction"),
        ContextParam("U1_MEAS", str, default="U1_RECTANGLE", help="U1 measurement type"),
        ContextParam("U1_ALPHA", str, default="0.07957753876221914", help="U1 alpha parameter"),
        ContextParam("U1_CHARGE", str, default="-1.0", help="U1 charge"),
        ContextParam("CONFIG_INFO", str, default="2+1DWF_b2.25_TEST", help="Configuration info"),
        ContextParam("STORAGE", str, default="CERN", help="Storage type"),
        ContextParam("BETA", str, default="6.0", help="Beta parameter"),
        ContextParam("ITERS", str, default="1500", help="Iterations"),
        ContextParam("MEASURE", str, default="1", help="Measure parameter"),
        ContextParam("OVER_ITERS", str, default="4", help="Over iterations"),
        ContextParam("SAVE", str, default="25", help="Save frequency"),
        ContextParam("THERM", str, default="100", help="Thermalization"),
    ]

    def _build_context(self, backend, ensemble_id: int, ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Build GLU input. All 38 schema params auto-merged and stringified."""
        ensure_keys(physics, ["L", "T"])
        
        # Return ONLY overrides and special values
        # (All input_params from schema auto-merged as strings)
        ensemble_dir = Path(ensemble["directory"]).resolve()
        # Keep CONFNO aligned with the starting config for smear/wflow jobs
        confno = job_params.get("config_start")
        overrides = {}
        if confno is not None:
            overrides["CONFNO"] = str(confno)
        
        return {
            # Override lattice dimensions (computed from ensemble)
            "DIM_0": str(physics["L"]),
            "DIM_1": str(physics["L"]),
            "DIM_2": str(physics["L"]),
            "DIM_3": str(physics["T"]),
            # Override confno if provided
            **overrides,
            # Template control
            "_output_dir": str(ensemble_dir),
            "_output_prefix": "glu_smear",
        }


__all__ = ["GluContextBuilder"]

