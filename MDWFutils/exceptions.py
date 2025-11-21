"""Custom exception hierarchy for MDWF utilities."""


class MDWFError(Exception):
    """Base exception for all MDWF errors."""


class EnsembleNotFoundError(MDWFError):
    """Raised when ensemble lookup fails."""

    def __init__(self, identifier):
        super().__init__(f"Ensemble not found: {identifier}")
        self.identifier = identifier


class ValidationError(MDWFError):
    """Raised when parameter validation fails."""


class TemplateError(MDWFError):
    """Raised when template rendering fails."""


class DatabaseError(MDWFError):
    """Raised when database operation fails."""


class ConnectionError(DatabaseError):
    """Raised when database connection fails."""


