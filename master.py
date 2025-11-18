import argparse
import logging

import script_a
import script_b


ASCII_BANNER = r"""
   _____                   _               _____
  / ____|                 | |             / ____|
 | |  __ _   _ _ __   ___ | |__   __ _   | (___   ___ _ __ __ _ _ __   ___ _ __
 | | |_ | | | | '_ \ / _ \| '_ \ / _` |   \___ \ / __| '__/ _` | '_ \ / _ \ '__|
 | |__| | |_| | | | | (_) | |_) | (_| |   ____) | (__| | | (_| | |_) |  __/ |
  \_____|\__,_|_| |_|\___/|_.__/ \__,_|  |_____/ \___|_|  \__,_| .__/ \___|_|
                                                               | |
                                                               |_|
"""


class BannerArgumentParser(argparse.ArgumentParser):
    def format_help(self) -> str:
        return f"{ASCII_BANNER}\n{super().format_help()}"


def determine_log_level(verbose: bool, debug: bool) -> int:
    if debug:
        return logging.DEBUG
    if verbose:
        return logging.INFO
    return logging.WARNING


def configure_logging(level: int) -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=level, format="%(asctime)s - %(message)s")
    else:
        root.setLevel(level)
        for handler in root.handlers:
            handler.setLevel(level)

    if hasattr(script_a, "configure_logging"):
        script_a.configure_logging(level)
    if hasattr(script_b, "configure_logging"):
        script_b.configure_logging(level)


def build_parser() -> argparse.ArgumentParser:
    parser = BannerArgumentParser(
        description='Guncad Scraper: Search + Deep Scrape v2.0',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  python master.py -x                                   # Stage1 scans site gets internal & external url save to db\n"
               "  python master.py -s                                   # Stage2 gets lbry:// and starts download of files\n"
               "  python master.py -s 1 -e 3 -o mylog.json              # start from page 1 end at 3 with a output to a json\n"
               "  python master.py -f -v                                # Fast mode with verbose logging\n"
               "  python master.py --db existing.db                     # save to existing DB\n"
    )

    parser.add_argument('-s', type=int, default=1, help='Start page (default: 1)')
    parser.add_argument('-e', type=int, default=233, help='End page (default: 233)')
    parser.add_argument('--db', type=str, default="guncad.db", help='Database path')
    parser.add_argument('-o', help='Export to JSON file')
    parser.add_argument('-c', type=int, default=5, help='Concurrent search requests')
    parser.add_argument('-d', type=int, default=3, dest='cd', help='Concurrent deep requests')
    parser.add_argument('-f', '--fast', action='store_true', dest='fast',
                        help='Fast mode (10 concurrent search, 0.5-1.5s delay)')
    parser.add_argument('-x', '--deep', action='store_true', dest='deep',
                        help='Enable Stage1 deep scraping of Odyssey detail pages')
    parser.add_argument('-s1', dest='stage1', action='store_true', default=True,
                        help='Enable Stage1 scraping (script_a) [default]')
    parser.add_argument('-nos1', dest='stage1', action='store_false',
                        help='Disable Stage1 (use existing DB + Stage2 only)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose logging (INFO level)')
    parser.add_argument('--debug', action='store_true',
                        help='Debug logging (overrides --verbose)')
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    log_level = determine_log_level(args.verbose, args.debug)
    configure_logging(log_level)
    print(ASCII_BANNER)
    result = None
    if args.stage1:
        result = script_a.run_scraper(args)
    else:
        result = {
            "stats": {},
            "json_file": None,
            "db_path": args.db,
            "failed_details": [],
        }

    if args.stage2 and result:
        script_b.run_stage_two(result)


if __name__ == "__main__":
    main()