from oai import OAI
import argparse
import utils
import requests
import institutions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--institutions', '-i', nargs='*',
                        help="If argument is set, specifies specific institutions to crawl.", required=False)
    parser.add_argument('--crawl_time', '-ct', nargs=1,
                        help="If set, changes amount of time after which a new crawl is performed", required=False, default=24)
    parser.add_argument('--ignore_time', '-ig', default=False,
                        action="store_true", help="If set, ignores whether data has been harvested in the past 24 hours already.")
    parser.add_argument('--csv', '-csv', default=False,
                        action="store_true", help="If set, converts JSON output to CSV.")
    parser.add_argument('--compile_only', '-co', default=False,
                        help="If set, script doesn't run a crawl, only compiles all data files into one.", action="store_true")
    args = parser.parse_args()

    institutions_data = institutions.get()

    for institution in institutions_data:
        if args.institutions:
            if institution.id not in args.institutions:
                continue

        # In order not to crawl redundantly, by default we skip crawls from the past 24 hours
        if utils.crawled_recently(institution.id, args.crawl_time or 24) and not args.ignore_time:
            print("{} has been crawled in less than {} hours, continuing.".format(institution.id, args.crawl_time))
            continue

        if institution.id == 'mhm':
            # Missouri History Museum provides a data dump feed instead of an OAI feed
            data = requests.get(institution.url).json()
            metadata = data['records']
            skipped = 0
            utils.write_file("files/institutions/", metadata, institution.id, institution.name, 0, {})

        else:
            # Create OAI object based on input data
            feed = OAI(institution)

            # Crawl the feed and write output to JSON
            data, skipped, skipped_messages = feed.crawl()
            utils.write_file("files/institutions/", data, institution.id, institution.name, skipped, skipped_messages)

        print(f"Total records: {len(data)}")
        print(f"Total skipped: {skipped}")


if __name__ == "__main__":
    main()
