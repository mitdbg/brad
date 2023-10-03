import argparse
import random
from datetime import datetime, timedelta

from workload_utils.dataset_config import (
    MAX_MOVIE_ID_100GB,
    MAX_MOVIE_ID_20GB,
    MAX_MOVIE_ID_ORIGINAL,
    THEATRES_PER_SF,
    HOMES_PER_SF,
    SHOWING_DAYS,
    SHOWINGS_PER_MOVIE_PER_DAY,
    MOVIES_PER_DAY_MIN,
    MOVIES_PER_DAY_MAX,
    MIN_CAPACITY,
    MAX_CAPACITY,
    MIN_ORDERS_PER_SHOWING,
    MAX_ORDERS_PER_SHOWING,
    MIN_MOVIE_ID,
)


class Context:
    def __init__(self, args) -> None:
        self.args = args
        self.prng = random.Random(args.seed)
        self.location_range = args.location_max - args.location_min

        datetime_parts = args.showing_start_date.split("-")
        self.start_datetime = datetime(
            int(datetime_parts[0]), int(datetime_parts[1]), int(datetime_parts[2])
        )

        if args.dataset_type == "original":
            self.max_movie_id = MAX_MOVIE_ID_ORIGINAL
        elif args.dataset_type == "20gb":
            self.max_movie_id = MAX_MOVIE_ID_20GB
        elif args.dataset_type == "100gb":
            self.max_movie_id = MAX_MOVIE_ID_100GB
        else:
            raise RuntimeError(args.dataset_type)


def generate_homes(ctx: Context) -> int:
    total_homes = ctx.args.scale_factor * THEATRES_PER_SF
    with open("homes.csv", "w", encoding="UTF-8") as out:
        print("id|location_x|location_y", file=out)

        for t in range(HOMES_PER_SF * ctx.args.scale_factor):
            loc_x = ctx.prng.random() * ctx.location_range + ctx.args.location_min
            loc_y = ctx.prng.random() * ctx.location_range + ctx.args.location_min
            print(
                "{}|{:.4f}|{:.4f}".format(t, loc_x, loc_y),
                file=out,
            )
    return total_homes


def generate_theatres(ctx: Context) -> int:
    total_theatres = ctx.args.scale_factor * THEATRES_PER_SF
    with open("theatres.csv", "w", encoding="UTF-8") as out:
        print("id|name|location_x|location_y", file=out)

        for t in range(THEATRES_PER_SF * ctx.args.scale_factor):
            loc_x = ctx.prng.random() * ctx.location_range + ctx.args.location_min
            loc_y = ctx.prng.random() * ctx.location_range + ctx.args.location_min
            print(
                "{}|Theatre #{}|{:.4f}|{:.4f}".format(t, t, loc_x, loc_y),
                file=out,
            )
    return total_theatres


def generate_showings(ctx: Context, total_theatres: int) -> int:
    total_showings = 0

    with open("showings.csv", "w", encoding="UTF-8") as out:
        print("id|theatre_id|movie_id|date_time|total_capacity|seats_left", file=out)

        movie_id_range = range(MIN_MOVIE_ID, ctx.max_movie_id + 1)

        for t in range(total_theatres):
            for day_offset in range(SHOWING_DAYS):
                num_movies = ctx.prng.randint(MOVIES_PER_DAY_MIN, MOVIES_PER_DAY_MAX)
                movie_ids_to_show = ctx.prng.sample(movie_id_range, num_movies)

                for movie_id in movie_ids_to_show:
                    for _ in range(SHOWINGS_PER_MOVIE_PER_DAY):
                        hours = ctx.prng.randint(0, 23)
                        minutes = ctx.prng.randint(0, 59)

                        date_time = ctx.start_datetime + timedelta(
                            days=day_offset, hours=hours, minutes=minutes
                        )
                        capacity = ctx.prng.randint(MIN_CAPACITY, MAX_CAPACITY)
                        print(
                            "|".join(
                                [
                                    str(total_showings),  # A proxy for ID
                                    str(t),
                                    str(movie_id),
                                    date_time.strftime("%Y-%m-%d %H:%M:%S"),
                                    str(capacity),
                                    str(capacity),
                                ]
                            ),
                            file=out,
                        )
                        total_showings += 1

    return total_showings


def generate_ticket_orders(ctx: Context, total_showings: int) -> int:
    # For implementation simplicity, the ticket orders do not correspond to the
    # capacity generated in the showings table. We can change this later if
    # needed.

    total_orders = 0
    quantity_choices = list(range(1, 6 + 1))
    weights = [1] * len(quantity_choices)
    weights[0] = 5
    weights[1] = 10

    with open("ticket_orders.csv", "w", encoding="UTF-8") as out:
        print("id|showing_id|quantity|contact_name|location_x|location_y", file=out)

        for showing_id in range(total_showings):
            num_orders_for_showing = ctx.prng.randint(
                MIN_ORDERS_PER_SHOWING, MAX_ORDERS_PER_SHOWING
            )

            for _ in range(num_orders_for_showing):
                quantity = ctx.prng.choices(quantity_choices, weights=weights, k=1)[0]
                loc_x = ctx.prng.random() * ctx.location_range + ctx.args.location_min
                loc_y = ctx.prng.random() * ctx.location_range + ctx.args.location_min
                print(
                    "|".join(
                        [
                            str(total_orders),
                            str(showing_id),
                            str(quantity),
                            "P{}".format(total_orders),
                            "{:.4f}".format(loc_x),
                            "{:.4f}".format(loc_y),
                        ]
                    ),
                    file=out,
                )
                total_orders += 1

    return total_orders


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale-factor", type=int, default=1)
    parser.add_argument("--location-min", type=float, default=0.0)
    parser.add_argument("--location-max", type=float, default=1e6)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--showing-start-date", type=str, default="2023-07-17")
    parser.add_argument(
        "--dataset-type",
        type=str,
        choices=["original", "20gb", "100gb"],
        default="original",
    )
    args = parser.parse_args()

    # Scale
    # -----
    # Theatres: 1000 * SF
    # Homes: 100 * NUM_THEATRES
    #
    # Showings:
    # - Pre-populated with 1 year's worth of showings
    # - 2 showings per day for 3 to 8 movies
    # - 365 * 2 * 8 * theatres
    #
    # Ticket orders:
    # - Pre-populated with 0-15 orders per showing
    #
    # Sizing:
    # SF = 1: ~1.7 GB (uncompressed, text format) (used for "original")
    # SF = 6: ~10.2 GB (uncompressed, text format) (used for "20gb")
    # SF = 33: ~56.1 GB (uncompressed, text format) (used for "100gb")

    print("Scale factor:", args.scale_factor)

    ctx = Context(args)
    print("Generating theatres...")
    total_theatres = generate_theatres(ctx)
    print("Generating showings...")
    total_showings = generate_showings(ctx, total_theatres)
    print("Generating ticket orders...")
    generate_ticket_orders(ctx, total_showings)
    print("Generating homes...")
    generate_homes(ctx)


if __name__ == "__main__":
    main()
