import argparse

from .exercises import ConflictEquivalentExercise

class Script():

    def create_parser(self):
        parser = argparse.ArgumentParser(description="DBTP Script")
        subparsers = parser.add_subparsers(dest="command")
        
        # Add subparser for the conflict-equivalency exercise
        ConflictEquivalentExercise.create_parser(subparsers)

        return parser

    def run(self):
        parser = self.create_parser()
        args = parser.parse_args()

        if args.command == "conf-eq":
            exercise = ConflictEquivalentExercise()
            exercise.parse_args(args)

            exercise.print_banner()            
            exercise.generate()

if __name__ == "__main__":
    script = Script()
    script.run()