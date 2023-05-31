# iqfeed-historical-pull-ohlcv.py

# iterates through all symbols pulled with iq-feed-symbol-scraping.py and outputs them in the same directory structure

# based on various repos, eventually have docker wine going, right now just used to pull data, full pipeline eventually
# https://www.youtube.com/watch?v=eA7085D0uls
# https://github.com/luketighe/IQFeed/blob/master/iqfeed.py
# https://www.quantstart.com/articles/Downloading-Historical-Intraday-US-Equities-From-DTN-IQFeed-with-Python/
# https://github.com/Quantmatic/iqfeed-docker
#  added multiprocessing, rate limiting...

import multiprocessing
import sys
import re
from pathlib import Path
import time
from time import sleep
import concurrent.futures
import socket
from datetime import datetime
from math import ceil
import os

import pandas as pd

from ratemate import RateLimit


class DataRequestError(Exception):
    """
    Custom Exception for Handling IQFeed Client Errors
    """

    def __init__(self, symbol, message):
        self.symbol = symbol
        self.message = message
        # super().__init__(self, message, symbol)


class IQFeedSocket:
    """
    Basic socket class
    """

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.timeout = 60

    def set_timeout(self, timeout):
        """
        Set socket timeout
        """
        self.timeout = timeout

    # Historical data socket is 9100, port #5009 is for streaming live data
    def connect(self, host="localhost", port=9100):
        """
        Make connection on host and port
        """
        self.sock.settimeout(self.timeout)

        attempt = 0
        while attempt < 10:
            try:
                self.sock.connect((host, port))
                break
            except socket.error as e:
                if attempt < 10:
                    attempt += 1
                    print(
                        f"Exception creating socket, trying again - attempt {attempt}",
                        file=sys.stderr,
                    )
                    sleep(3)
                    continue
                raise Exception from e

    def send(self, message):
        """
        Send message to socket
        """

        # Construct the message needed by IQFeed to retrieve data
        message = str.encode(message)
        # Send the historical data request
        # message and buffer the data
        sent = self.sock.sendall(message)
        if sent == 0:
            raise RuntimeError("socket send - socket connection broken")

    def receive(self, recv_buffer=33554432, debug=False):  # 32 mb
        """
        Receive message from socket
        """

        buffer = ""
        data = ""
        while True:
            data = self.sock.recv(recv_buffer)
            buffer += data.decode()

            if debug:
                print(buffer)

            # Check if the end message string arrives
            if "ENDMSG" in buffer:
                break

            if len(buffer) == 0:
                raise RuntimeError("socket receive - socket connection broken")

        # Remove the end message string
        buffer = buffer[:-12]
        return buffer

    def close_and_shutdown(self):
        """
        Close and shutdown socket connection
        """
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()


def read_historical_data_socket(message):
    """
    Initialize socket, send message, receive data
    """

    closed = False
    requested_data = ""
    try:
        sock = IQFeedSocket()
        sock.set_timeout(900)  # 15 minutes
        sock.connect()
        sock.send(message)

        # # Remove all the endlines and line-ending
        # # comma delimiter from each record
        requested_data = sock.receive()
        requested_data = "".join(requested_data.split("\r"))
        requested_data = requested_data.replace(",\n", "\n")[:-1]

        sock.close_and_shutdown()
        closed = True
    except socket.timeout:
        print("Socket timed out")
    finally:
        if not closed:
            sock.close_and_shutdown()
        return requested_data


def chunkify(l, n):
    # len_ = len(l)
    # split_size = len_ // n
    # split_size = n if not split_size else split_size
    # offsets = [i for i in range(0, len_, split_size)]
    # return [l[offset:offset + split_size] for offset in offsets]
    return [l]


def pull_data(symbol, exchange_security, timeframe, output_dir):
    # temp = random.randint(1, 5)
    # sleep(temp)
    # print(f"sleeping for {temp}")

    # if (temp == 3):
    #     raise DataRequestError("meep ---- noooo ---- ", "what the fuck!!!")
    # return str(1) + "-----meepmeep-----" + str(temp)

    # when scraping the symbols some had an invalid character that can't be in a filename
    symbol_processed = re.sub(r'[/:*?"<>|]', "invalid-char", symbol)
    filename = symbol_processed + ".csv"
    complete_path = output_dir / filename

    startDate = "19000101"
    endDate = "20221224"

    dataDirection = "1"  # 1 is oldest to newest, 0 is newest to oldest

    # minute/daily/weekly data format
    # Timestamp, HLOC, TotalVolume, Volume
    if timeframe == "minute":
        interval = "60"
        # minute data message
        message = f"HIT,{symbol},{interval},{startDate},{endDate},,000000,235959,{dataDirection}\n"
    elif timeframe == "daily":
        # daily data message
        message = f"HDT,{symbol},{startDate},{endDate},,{dataDirection}\n"
    elif timeframe == "weekly":
        maxDataPoints = 6500  # should be all the way to 1900 and then some
        # weekly data message
        message = f"HWX,{symbol},{maxDataPoints},{dataDirection}\n"
    elif timeframe == "tick":
        # # tick start end
        startDate = "20220101 000000"
        endDate = "20221224 235959"
        # tick data message
        message = f"HTT,{symbol},{startDate},{endDate},,000000,235959,{dataDirection}\n"

    no_data_error = f"no-data for symbol {symbol} exchange_security {exchange_security}"
    unauthorized_error = (
        f"unauthorized symbol {symbol} exchange_security {exchange_security}"
    )
    too_many_requests_error = (
        f"too many requests symbol {symbol} exchange_security {exchange_security}"
    )
    connection_timeout_error = f"connection timeout error symbol {symbol}  exchange_security {exchange_security}"
    socket_error = f"socket error symbol {symbol} exchange_security {exchange_security}"
    unrecognized_error = (
        f"unrecognized error symbol {symbol}  exchange_security {exchange_security}"
    )
    unknown_server_error = (
        f"unknown server error symbol {symbol}  exchange_security {exchange_security}"
    )

    data = read_historical_data_socket(message)

    if "!NO_DATA!" in data[:50]:
        print(no_data_error)
        raise DataRequestError(symbol, no_data_error)
    if "Unauthorized user ID." in data[:50]:
        print(unauthorized_error)
        raise DataRequestError(symbol, unauthorized_error)
    if "socket" in data[:50].lower():
        print(socket_error)
        raise DataRequestError(symbol, socket_error)
    if "simultaneous" in data[:50].lower():
        print(too_many_requests_error)
        raise DataRequestError(symbol, too_many_requests_error)
    if (
        "connection timeout" in data[:50].lower()
        or "connection timeout" in data[-50:].lower()
    ):
        print(connection_timeout_error)
        raise DataRequestError(symbol, connection_timeout_error)
    if (
        "unknown server error" in data[:50].lower()
        or "unknown server error" in data[-50:].lower()
    ):
        print(unknown_server_error)
        raise DataRequestError(symbol, unknown_server_error)
    if "E," in data[:50] or "E," in data[-50:]:
        print(unrecognized_error)
        raise DataRequestError(symbol, unrecognized_error)

    if not Path.exists(output_dir):
        Path.mkdir(output_dir, parents=True, exist_ok=True)
    # Write the data stream to disk
    with open(complete_path, "w", encoding="utf-8") as f:
        f.write(data)

    return f"wrote data for {complete_path}"


# https://www.youtube.com/watch?v=eA7085D0uls
# https://github.com/luketighe/IQFeed/blob/master/iqfeed.py
# https://www.quantstart.com/articles/Downloading-Historical-Intraday-US-Equities-From-DTN-IQFeed-with-Python/
# https://github.com/Quantmatic/iqfeed-docker
if __name__ == "__main__":
    tic = time.perf_counter()

    rate_limit = RateLimit(max_count=40, per=3, greedy=True)
    # rate_limit = RateLimit(max_count=10, per=2, greedy=True)

    # Get the list of all files and directories
    # structure is ...exchange/security/...csv
    # symbol_list_path = ".../symbols/iqfeed-symbols"
    # symbol_list = Path(symbol_list_path)
    symbol_list_path = Path(".../symbols/iqfeed-symbols")
    # symbol_list_path = Path("B:/stonks/symbols/symbol-list-20221026")

    # timeframe_list = ["weekly", "daily", "minute"]
    # timeframe_list = ["tick"]
    timeframe_list = ["daily", "weekly", "minute"]

    today = datetime.today()
    day = "23"
    month = "12"
    year = "2022"
    date_str = year + "/" + month + "/" + day

    overwrite = False
    ignore_symbols_in_path = True
    discard_ignored_symbol_list = False

    options_data_pull = False

    if options_data_pull:
        symbol_list_path = symbol_list_path / "-options"

    #
    # get a dictionary with exchange, security as key and symbol list as val
    #

    wildcard_path = "*.csv"

    files = symbol_list_path.rglob(wildcard_path)

    # combine it all into a dataframe
    df = pd.DataFrame()

    for f in files:
        df2 = pd.read_csv(f, sep=",")
        df = pd.concat([df, df2], ignore_index=True)

    # group by exchange and security type
    temp_dict = dict(list(df.groupby(["ExchangeType", "SecurityType"])))
    exchange_security_dict = {}

    for key in temp_dict.keys():
        exchange_security_symbol_list_temp = temp_dict[key]["Symbol"].tolist()
        exchange_security_symbol_list = chunkify(
            exchange_security_symbol_list_temp,
            ceil(len(exchange_security_symbol_list_temp) / 250),
        )
        exchange_security_dict[key] = exchange_security_symbol_list

    exchange_security_type_list = exchange_security_dict.keys()

    # get list of symbols to ignore (either no data or unauthorized)

    symbol_to_ignore_path = Path(".../symbols/iqfeed-symbols-to-ignore")

    if not Path.exists(symbol_to_ignore_path):
        Path.mkdir(symbol_to_ignore_path, parents=True, exist_ok=True)

    no_data_weekly_path = symbol_to_ignore_path / "no-data-weekly.csv"
    unauthorized_weekly_path = symbol_to_ignore_path / "unauthorized-weekly.csv"

    no_data_daily_path = symbol_to_ignore_path / "no-data-daily.csv"
    unauthorized_daily_path = symbol_to_ignore_path / "unauthorized-daily.csv"

    no_data_minute_or_tick_path = symbol_to_ignore_path / "no-data-minute-or-tick.csv"
    unauthorized_minute_or_tick_path = (
        symbol_to_ignore_path / "unauthorized-minute-or-tick.csv"
    )

    # no_data_tick_path = symbol_to_ignore_path / "no-data-tick.csv"
    # unauthorized_tick_path = symbol_to_ignore_path / "unauthorized-tick.csv"

    need_to_check_again_path = symbol_to_ignore_path / "check-again.csv"

    # tick_no_data_symbols_list = []
    # tick_unauthorized_symbols_list = []

    minute_or_tick_no_data_symbols_list = []
    minute_or_tick_unauthorized_symbols_list = []

    daily_no_data_symbols_list = []
    daily_unauthorized_symbols_list = []

    weekly_no_data_symbols_list = []
    weekly_unauthorized_symbols_list = []

    need_to_check_again_list = []

    if Path.exists(need_to_check_again_path):
        need_to_check_again_list = pd.read_csv(need_to_check_again_path)[
            "Symbol"
        ].tolist()
    else:
        with open(need_to_check_again_path, "w", encoding="utf-8") as f:
            f.write("Symbol\n")

    if Path.exists(no_data_weekly_path):
        weekly_no_data_symbols_list = pd.read_csv(no_data_weekly_path)[
            "Symbol"
        ].tolist()
    else:
        with open(no_data_weekly_path, "w", encoding="utf-8") as f:
            f.write("Symbol\n")
    if Path.exists(unauthorized_weekly_path):
        weekly_unauthorized_symbols_list = pd.read_csv(unauthorized_weekly_path)[
            "Symbol"
        ].tolist()
    else:
        with open(unauthorized_weekly_path, "w", encoding="utf-8") as f:
            f.write("Symbol\n")
    weekly_ignore_symbol_list = (
        weekly_no_data_symbols_list + weekly_unauthorized_symbols_list
    )

    if Path.exists(no_data_daily_path):
        daily_no_data_symbols_list = pd.read_csv(no_data_daily_path)["Symbol"].tolist()
    else:
        with open(no_data_daily_path, "w", encoding="utf-8") as f:
            f.write("Symbol\n")
    if Path.exists(unauthorized_daily_path):
        daily_unauthorized_symbols_list = pd.read_csv(unauthorized_daily_path)[
            "Symbol"
        ].tolist()
    else:
        with open(unauthorized_daily_path, "w", encoding="utf-8") as f:
            f.write("Symbol\n")
    daily_ignore_symbol_list = (
        daily_no_data_symbols_list + daily_unauthorized_symbols_list
    )

    if Path.exists(no_data_minute_or_tick_path):
        minute_or_tick_no_data_symbols_list = pd.read_csv(no_data_minute_or_tick_path)[
            "Symbol"
        ].tolist()
    else:
        with open(no_data_minute_or_tick_path, "w", encoding="utf-8") as f:
            f.write("Symbol\n")
    if Path.exists(unauthorized_minute_or_tick_path):
        minute_or_tick_unauthorized_symbols_list = pd.read_csv(
            unauthorized_minute_or_tick_path
        )["Symbol"].tolist()
    else:
        with open(unauthorized_minute_or_tick_path, "w", encoding="utf-8") as f:
            f.write("Symbol\n")
    minute_or_tick_ignore_symbol_list = (
        minute_or_tick_no_data_symbols_list + minute_or_tick_unauthorized_symbols_list
    )

    # if Path.exists(no_data_tick_path):
    #     tick_no_data_symbols_list = pd.read_csv(
    #         no_data_tick_path)["Symbol"].tolist()
    # else:
    #     with open(no_data_tick_path, "w", encoding="utf-8") as f:
    #         f.write("Symbol\n")
    # if Path.exists(unauthorized_tick_path):
    #     tick_unauthorized_symbols_list = pd.read_csv(
    #         unauthorized_tick_path)["Symbol"].tolist()
    # else:
    #     with open(no_data_tick_path, "w", encoding="utf-8") as f:
    #         f.write("Symbol\n")
    # tick_ignore_symbol_list = tick_no_data_symbols_list + tick_unauthorized_symbols_list

    count = 0
    minute_or_tick_no_data_count = 0
    # tick_no_data_count = 0
    daily_no_data_count = 0
    weekly_no_data_count = 0

    minute_or_tick_unauthorized_count = 0
    # tick_unauthorized_count = 0
    daily_unauthorized_count = 0
    weekly_unauthorized_count = 0

    minute_manager = multiprocessing.Manager()
    # tick_manager = multiprocessing.Manager()
    daily_manager = multiprocessing.Manager()
    weekly_manager = multiprocessing.Manager()
    need_to_check_again_manager = multiprocessing.Manager()

    minute_manager_lock = minute_manager.Lock()
    # tick_manager_lock = tick_manager.Lock()
    daily_manager_lock = daily_manager.Lock()
    weekly_manager_lock = weekly_manager.Lock()
    need_to_check_again_manager_lock = need_to_check_again_manager.Lock()

    # multiprocessing
    with concurrent.futures.ProcessPoolExecutor(max_workers=50) as executor:
        results = {}
        for timeframe in timeframe_list:
            # firstloop = True
            symbols_to_ignore = []
            if timeframe in ("minute", "tick"):
                # minute data message
                symbols_to_ignore = minute_or_tick_ignore_symbol_list
            elif timeframe == "daily":
                # daily data message
                symbols_to_ignore = daily_ignore_symbol_list
            elif timeframe == "weekly":
                # weekly data message
                symbols_to_ignore = weekly_ignore_symbol_list
            # elif timeframe == "tick":
            #     # tick data message
            #     symbols_to_ignore = tick_ignore_symbol_list

            for exchange_security in exchange_security_type_list:
                exchange = exchange_security[0]
                security = exchange_security[1]

                # security_type_list = ["future", "equity"]

                # if security not in security_type_list:
                #     continue

                # exchanges_list = ["cme"]

                # if (exchange not in exchanges_list):
                #     continue

                # if security in ("generic-report"):
                #     continue

                # if "future" not in security:
                #     continue

                # if exchange in ("usda", "baltic", "cftc"):
                #     continue

                output_dir_path = Path(".../iqfeed-data")

                if options_data_pull:
                    output_dir_path = output_dir_path / "-options"

                output_dir_path = output_dir_path / timeframe / security / date_str

                if discard_ignored_symbol_list:
                    symbols_to_ignore = []

                symbol_data_already_exists_list = []
                if not overwrite:
                    if Path.exists(output_dir_path):
                        # print("meep path exists")
                        # wildcard_path = "*.csv"
                        # files_in_dir = [*output_dir_path.rglob(wildcard_path)]
                        # files_in_dir = [str(x) for x in files_in_dir]
                        files_in_dir = os.listdir(output_dir_path)
                        symbol_data_already_exists_list = [
                            x.replace(".csv", "") for x in files_in_dir
                        ]
                for chunk in exchange_security_dict[exchange_security]:
                    skipped = 0
                    already_exists = 0
                    for symbol in chunk:
                        # if not firstloop:
                        #     continue
                        # symbol = "@ES#"
                        # firstloop = False
                        if symbol in symbols_to_ignore:
                            skipped += 1
                            continue
                        if ignore_symbols_in_path:
                            if symbol in symbol_data_already_exists_list:
                                already_exists += 1
                                continue
                        rate_limit.wait()
                        results[
                            executor.submit(
                                pull_data,
                                symbol,
                                exchange_security,
                                timeframe,
                                output_dir_path,
                            )
                        ] = symbol
                        count += 1
                        print(
                            f"count is {count}, time is {time.perf_counter() - tic}, exchange {exchange} - security {security} - symbol is {symbol}"
                        )
                    if skipped != 0:
                        print(f"Ignored {skipped} symbols for {exchange} - {security}")
                    if already_exists != 0:
                        print(
                            f"Already exists - Ignored {already_exists} symbols for {exchange} - {security}"
                        )
                    for result in concurrent.futures.as_completed(results):
                        try:
                            outcome = result.result()
                            print(f"checked {results[result]} - result {outcome}")
                        except DataRequestError as e:
                            symbol = e.symbol
                            error_message = e.message
                            print(f"{error_message}", file=sys.stderr)
                            if "no-data" in error_message:
                                if timeframe in ("minute", "tick"):
                                    minute_or_tick_no_data_count += 1
                                    with minute_manager_lock:
                                        with open(
                                            no_data_minute_or_tick_path,
                                            "a",
                                            encoding="utf-8",
                                        ) as f:
                                            f.write(f"{symbol}\n")
                                elif timeframe == "daily":
                                    daily_no_data_count += 1
                                    with daily_manager_lock:
                                        with open(
                                            no_data_daily_path, "a", encoding="utf-8"
                                        ) as f:
                                            f.write(f"{symbol}\n")
                                elif timeframe == "weekly":
                                    weekly_no_data_count += 1
                                    with weekly_manager_lock:
                                        with open(
                                            no_data_weekly_path, "a", encoding="utf-8"
                                        ) as f:
                                            f.write(f"{symbol}\n")
                            elif "unauthorized" in error_message:
                                if timeframe in ("minute", "tick"):
                                    minute_or_tick_unauthorized_count += 1
                                    with minute_manager_lock:
                                        with open(
                                            unauthorized_minute_or_tick_path,
                                            "a",
                                            encoding="utf-8",
                                        ) as f:
                                            f.write(f"{symbol}\n")
                                elif timeframe == "daily":
                                    daily_unauthorized_count += 1
                                    with daily_manager_lock:
                                        with open(
                                            unauthorized_daily_path,
                                            "a",
                                            encoding="utf-8",
                                        ) as f:
                                            f.write(f"{symbol}\n")
                                elif timeframe == "weekly":
                                    weekly_unauthorized_count += 1
                                    with weekly_manager_lock:
                                        with open(
                                            unauthorized_weekly_path,
                                            "a",
                                            encoding="utf-8",
                                        ) as f:
                                            f.write(f"{symbol}\n")
                            error_types_list = [
                                "socket",
                                "too many requests",
                                "connection timeout",
                                "unknown server error",
                                "unrecognized error",
                            ]
                            for error in error_types_list:
                                if error in error_message:
                                    print(
                                        f"{timeframe} - {symbol} - {error} in {error_message}, writing to check again",
                                        file=sys.stderr,
                                    )
                                    with need_to_check_again_manager_lock:
                                        with open(
                                            need_to_check_again_path,
                                            "a",
                                            encoding="utf-8",
                                        ) as f:
                                            f.write(f"{symbol}\n")
                                        need_to_check_again_list.append(symbol)
                        except Exception as e:
                            print(e, file=sys.stderr)

        print(f"\n\n{timeframe} completed\n\n")

    #     if timeframe in ("minute", "tick"):
    #         no_data_minute_or_tick_path = symbol_to_ignore_path / "no-data-minute-or-tick.csv"
    #         unauthorized_minute_or_tick_path = symbol_to_ignore_path / "unauthorized-minute-or-tick.csv"
    #         if not len(minute_or_tick_no_data_symbols_list) == 0:
    #             pd.DataFrame.from_dict(
    #                 {"Symbol": minute_or_tick_no_data_symbols_list}
    #             ).to_csv(no_data_minute_or_tick_path, index=False)
    #         if not len(minute_or_tick_unauthorized_symbols_list) == 0:
    #             pd.DataFrame.from_dict(
    #                 {"Symbol": minute_or_tick_unauthorized_symbols_list}
    #             ).to_csv(unauthorized_minute_or_tick_path, index=False)
    #     elif timeframe == "daily":
    #         # daily data message
    #         no_data_daily_path = symbol_to_ignore_path / "no-data-daily.csv"
    #         unauthorized_daily_path = symbol_to_ignore_path / "unauthorized-daily.csv"
    #         if not len(daily_no_data_symbols_list) == 0:
    #             pd.DataFrame.from_dict(
    #                 {"Symbol": daily_no_data_symbols_list}
    #             ).to_csv(no_data_daily_path, index=False)
    #         if not len(daily_unauthorized_symbols_list) == 0:
    #             pd.DataFrame.from_dict(
    #                 {"Symbol": daily_unauthorized_symbols_list}
    #             ).to_csv(unauthorized_daily_path, index=False)

    #         # del daily_no_data_symbols_list
    #         # del daily_unauthorized_symbols_list
    #     elif timeframe == "weekly":
    #         # weekly data message
    #         no_data_weekly_path = symbol_to_ignore_path / "no-data-weekly.csv"
    #         unauthorized_weekly_path = symbol_to_ignore_path / "unauthorized-weekly.csv"
    #         if not len(weekly_no_data_symbols_list) == 0:
    #             pd.DataFrame.from_dict(
    #                 {"Symbol": weekly_no_data_symbols_list}
    #             ).to_csv(no_data_weekly_path, index=False)
    #         if not len(weekly_unauthorized_symbols_list) == 0:
    #             pd.DataFrame.from_dict(
    #                 {"Symbol": weekly_unauthorized_symbols_list}
    #             ).to_csv(unauthorized_weekly_path, index=False)
    #     # elif timeframe == "tick":
    #     #     # tick data message
    #     #     no_data_tick_path = symbol_to_ignore_path / "no-data-tick.csv"
    #     #     unauthorized_tick_path = symbol_to_ignore_path / "unauthorized-tick.csv"
    #     #     if not len(tick_no_data_symbols_list) == 0:
    #     #         pd.DataFrame.from_dict(
    #     #             {"Symbol": tick_no_data_symbols_list}
    #     #         ).to_csv(no_data_tick_path, index=False)
    #     #     if not len(tick_unauthorized_symbols_list) == 0:
    #     #         pd.DataFrame.from_dict(
    #     #             {"Symbol": tick_unauthorized_symbols_list}
    #     #         ).to_csv(unauthorized_tick_path, index=False)

    # if len(need_to_check_again_list) != 0:
    #     need_to_check_again_path = symbol_to_ignore_path / "check-again.csv"
    #     pd.DataFrame.from_dict(
    #         {"Symbol": need_to_check_again_list}
    #     ).to_csv(need_to_check_again_path, index=False)
    # else:
    #     print("need_to_check_again_list is empty")

    # print(f"tick no data count {tick_no_data_count}")
    # print(f"tick unauthorized count {tick_unauthorized_count}")
    print(f"minute no data count {minute_or_tick_no_data_count}")
    print(f"minute unauthorized count {minute_or_tick_unauthorized_count}")
    print(f"daily no data count {daily_no_data_count}")
    print(f"daily unauthorized count {daily_unauthorized_count}")
    print(f"weekly no data count {weekly_no_data_count}")
    print(f"weekly unauthorized count {weekly_unauthorized_count}")

    subtract_from_count = (
        minute_or_tick_no_data_count
        + minute_or_tick_unauthorized_count
        + daily_no_data_count
        + daily_unauthorized_count
        + weekly_no_data_count
        + weekly_unauthorized_count
    )

    print(f"pulled data for {count - subtract_from_count} symbols!!!")

    toc = time.perf_counter()

    print(f"took {toc - tic} seconds")
