from dagster import RetryPolicy, Backoff

API_RETRY_POLICY = RetryPolicy(max_retries=3,
                                      delay = 4,
                                      backoff=Backoff.EXPONENTIAL)