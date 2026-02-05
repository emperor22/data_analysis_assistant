class RetryableValidationException(Exception):
    pass

class BlacklistedDatasetException(Exception):
    pass

class FileReadException(Exception):
    pass

class InvalidDatasetException(Exception):
    pass

class RateLimitedException(Exception):
    pass

class RetryableRateLimitException(Exception):
    pass

class TerminalRateLimitException(Exception):
    pass