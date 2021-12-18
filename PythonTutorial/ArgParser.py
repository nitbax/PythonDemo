from argparse import ArgumentParser
import math

parser = ArgumentParser(description="Calculate the volume of a cylinder")
parser.add_argument(
    "-r",
    "--radius",
    metavar="",
    required=True,
    help="Provide the radius of the cylinder",
    type=int,
)
parser.add_argument(
    "-H",
    "--height",
    metavar="",
    required=True,
    help="Provide the height of the cylinder",
    type=int,
)
parser.add_argument(
    "-c",
    "--comment",
    metavar="",
    help="Provide the comment you want to print",
    type=str,
)
args = parser.parse_args()


def volume_of_cylinder(radius, height, msg) -> float:
    vol = math.pi * (radius ** 2) * height
    if msg is not None:
        rVal = msg + str(vol)
    else:
        rVal = str(vol)
    return rVal


if __name__ == "__main__":
    print(volume_of_cylinder(args.radius, args.height, args.comment))
