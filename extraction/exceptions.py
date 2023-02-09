""" Exceptions for the extraction module. """
class IncompatibleTables(ValueError):
    pass
class NoCbCReportFound(ValueError):
    pass
class StandardizationError(ValueError):
    pass
class MetadataError(AttributeError):
    pass
class RulesError(ValueError):
    pass