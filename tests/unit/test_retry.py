"""Unit tests for retry logic."""

import asyncio
import time
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from sql_databricks_bridge.sync.retry import (
    RetryExhaustedError,
    calculate_backoff,
    retry_async,
    retry_sync,
    with_retry,
)


class TestRetryExhaustedError:
    """Tests for RetryExhaustedError."""

    def test_error_with_all_params(self):
        """Error stores all parameters."""
        original_error = ValueError("original")
        error = RetryExhaustedError(
            "All attempts failed",
            attempts=3,
            last_error=original_error,
        )

        assert str(error) == "All attempts failed"
        assert error.attempts == 3
        assert error.last_error is original_error

    def test_error_without_last_error(self):
        """Error works without last_error."""
        error = RetryExhaustedError("Failed", attempts=5)

        assert error.attempts == 5
        assert error.last_error is None


class TestCalculateBackoff:
    """Tests for calculate_backoff function."""

    def test_exponential_growth(self):
        """Backoff grows exponentially."""
        with patch("sql_databricks_bridge.sync.retry.random.uniform", return_value=0):
            delays = [calculate_backoff(i, base_delay=1.0, jitter=False) for i in range(5)]

        assert delays[0] == 1.0  # 1 * 2^0
        assert delays[1] == 2.0  # 1 * 2^1
        assert delays[2] == 4.0  # 1 * 2^2
        assert delays[3] == 8.0  # 1 * 2^3
        assert delays[4] == 16.0  # 1 * 2^4

    def test_max_delay_cap(self):
        """Delay is capped at max_delay."""
        delay = calculate_backoff(10, base_delay=1.0, max_delay=30.0, jitter=False)

        assert delay == 30.0

    def test_jitter_adds_variation(self):
        """Jitter adds randomness to delay."""
        delays = [calculate_backoff(0, base_delay=10.0, jitter=True) for _ in range(10)]

        # With jitter, not all delays should be exactly the same
        assert len(set(delays)) > 1

    def test_jitter_within_range(self):
        """Jitter stays within ±25% range."""
        base = 10.0
        for _ in range(100):
            delay = calculate_backoff(0, base_delay=base, jitter=True)
            # Should be within ±25% of base
            assert base * 0.75 <= delay <= base * 1.25

    def test_custom_base_delay(self):
        """Custom base delay is respected."""
        delay = calculate_backoff(0, base_delay=5.0, jitter=False)
        assert delay == 5.0

    def test_zero_attempt(self):
        """First attempt (0) uses base delay."""
        delay = calculate_backoff(0, base_delay=2.0, jitter=False)
        assert delay == 2.0


class TestRetryAsync:
    """Tests for retry_async function."""

    @pytest.mark.asyncio
    async def test_success_first_attempt(self):
        """Function succeeds on first attempt."""
        mock_func = AsyncMock(return_value="success")

        result = await retry_async(mock_func, max_attempts=3)

        assert result == "success"
        assert mock_func.call_count == 1

    @pytest.mark.asyncio
    async def test_success_after_retry(self):
        """Function succeeds after initial failure."""
        mock_func = AsyncMock(side_effect=[ValueError("fail"), "success"])

        with patch("sql_databricks_bridge.sync.retry.asyncio.sleep"):
            result = await retry_async(mock_func, max_attempts=3, base_delay=0.01)

        assert result == "success"
        assert mock_func.call_count == 2

    @pytest.mark.asyncio
    async def test_exhausted_retries(self):
        """All retries exhausted raises error."""
        mock_func = AsyncMock(side_effect=ValueError("always fails"))

        with patch("sql_databricks_bridge.sync.retry.asyncio.sleep"):
            with pytest.raises(RetryExhaustedError) as exc_info:
                await retry_async(mock_func, max_attempts=3, base_delay=0.01)

        assert exc_info.value.attempts == 3
        assert isinstance(exc_info.value.last_error, ValueError)

    @pytest.mark.asyncio
    async def test_on_retry_callback(self):
        """on_retry callback is called on each retry."""
        error1 = ValueError("fail1")
        error2 = ValueError("fail2")
        mock_func = AsyncMock(side_effect=[error1, error2, "success"])
        callback = MagicMock()

        with patch("sql_databricks_bridge.sync.retry.asyncio.sleep"):
            result = await retry_async(
                mock_func,
                max_attempts=3,
                base_delay=0.01,
                on_retry=callback,
            )

        assert result == "success"
        assert callback.call_count == 2
        # Verify callback was called with attempt number and error
        callback.assert_any_call(0, error1)
        callback.assert_any_call(1, error2)

    @pytest.mark.asyncio
    async def test_specific_retryable_exceptions(self):
        """Only specified exceptions trigger retry."""
        mock_func = AsyncMock(side_effect=TypeError("not retryable"))

        with pytest.raises(TypeError):
            await retry_async(
                mock_func,
                max_attempts=3,
                retryable_exceptions=(ValueError,),  # Only ValueError is retryable
            )

        assert mock_func.call_count == 1

    @pytest.mark.asyncio
    async def test_passes_args_and_kwargs(self):
        """Arguments are passed to the function."""
        mock_func = AsyncMock(return_value="result")

        await retry_async(mock_func, "arg1", "arg2", key="value", max_attempts=1)

        mock_func.assert_called_once_with("arg1", "arg2", key="value")


class TestRetrySync:
    """Tests for retry_sync function."""

    def test_success_first_attempt(self):
        """Function succeeds on first attempt."""
        mock_func = MagicMock(return_value="success")

        result = retry_sync(mock_func, max_attempts=3)

        assert result == "success"
        assert mock_func.call_count == 1

    def test_success_after_retry(self):
        """Function succeeds after initial failure."""
        mock_func = MagicMock(side_effect=[ValueError("fail"), "success"])

        with patch("time.sleep"):
            result = retry_sync(mock_func, max_attempts=3, base_delay=0.01)

        assert result == "success"
        assert mock_func.call_count == 2

    def test_exhausted_retries(self):
        """All retries exhausted raises error."""
        mock_func = MagicMock(side_effect=ValueError("always fails"))

        with patch("time.sleep"):
            with pytest.raises(RetryExhaustedError) as exc_info:
                retry_sync(mock_func, max_attempts=3, base_delay=0.01)

        assert exc_info.value.attempts == 3
        assert isinstance(exc_info.value.last_error, ValueError)

    def test_on_retry_callback(self):
        """on_retry callback is called on each retry."""
        mock_func = MagicMock(side_effect=[ValueError("fail"), "success"])
        callback = MagicMock()

        with patch("time.sleep"):
            result = retry_sync(
                mock_func,
                max_attempts=3,
                base_delay=0.01,
                on_retry=callback,
            )

        assert result == "success"
        assert callback.call_count == 1

    def test_specific_retryable_exceptions(self):
        """Only specified exceptions trigger retry."""
        mock_func = MagicMock(side_effect=TypeError("not retryable"))

        with pytest.raises(TypeError):
            retry_sync(
                mock_func,
                max_attempts=3,
                retryable_exceptions=(ValueError,),
            )

        assert mock_func.call_count == 1


class TestWithRetryDecorator:
    """Tests for with_retry decorator."""

    def test_decorator_success(self):
        """Decorated function succeeds."""
        @with_retry(max_attempts=3)
        def my_func():
            return "success"

        assert my_func() == "success"

    def test_decorator_retries_on_failure(self):
        """Decorated function retries on failure."""
        call_count = 0

        @with_retry(max_attempts=3, base_delay=0.01)
        def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("fail")
            return "success"

        with patch("time.sleep"):
            result = my_func()

        assert result == "success"
        assert call_count == 2

    def test_decorator_exhausted(self):
        """Decorated function exhausts retries."""
        @with_retry(max_attempts=2, base_delay=0.01)
        def my_func():
            raise ValueError("always fails")

        with patch("time.sleep"):
            with pytest.raises(RetryExhaustedError):
                my_func()

    def test_decorator_preserves_function_name(self):
        """Decorator preserves original function name."""
        @with_retry()
        def original_name():
            pass

        assert original_name.__name__ == "original_name"

    def test_decorator_with_args(self):
        """Decorated function receives arguments."""
        @with_retry(max_attempts=1)
        def my_func(a, b, c=None):
            return (a, b, c)

        result = my_func(1, 2, c=3)
        assert result == (1, 2, 3)

    def test_decorator_custom_exceptions(self):
        """Decorator only retries specified exceptions."""
        @with_retry(max_attempts=3, retryable_exceptions=(ValueError,))
        def my_func():
            raise TypeError("not retryable")

        with pytest.raises(TypeError):
            my_func()
