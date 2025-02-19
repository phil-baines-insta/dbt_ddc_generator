class CronConverter:
    @staticmethod
    def to_hourly(cron_expression: str) -> str:
        """
        Convert cron expression to hourly format (e.g., '24h')

        Args:
            cron_expression: Cron expression (e.g., "0 */2 * * *")

        Returns:
            str: Hourly format (e.g., "2h")
        """
        try:
            # Split cron expression
            minute, hour, _, _, _ = cron_expression.split()

            # Handle common patterns
            if hour.startswith("*/"):
                # For expressions like "0 */2 * * *"
                interval = int(hour.replace("*/", ""))
                return f"{interval}h"
            elif hour == "*":
                # For expressions like "0 * * * *"
                return "1h"
            else:
                # Default to 24h for any other pattern
                return "24h"
        except (ValueError, IndexError, AttributeError):
            # Return 24h as default if parsing fails
            return "24h"
