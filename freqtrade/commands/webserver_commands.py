from typing import Any, Dict

from freqtrade.enums import RunMode


def start_webserver(args: Dict[str, Any]) -> None:
    """
    Main entry point for webserver mode
    """
    from freqtrade.configuration import Configuration
    from freqtrade.rpc.api_server import ApiServer

    # Initialize configuration
    config = Configuration(args, RunMode.WEBSERVER).get_config()
    if args.get('db_history'):
        config['dataformat_ohlcv'] = "mysql"
        config['dataformat_trades'] = "mysql"
        config['db_history'] = args.get('db_history')
    ApiServer(config, standalone=True)
