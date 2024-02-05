import glob
import json, os
import shutil
import subprocess

from datetime import datetime

from threading import Lock


class Singleton(type):
    _instances = {}
    _lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Model:
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr[attr.rfind('__') + 2:], value

    def __str__(self):
        return f"{type(self).__name__}(\n" + ',\n'.join(
            [f"{attr[attr.rfind('__') + 1:]}={value}" for attr, value in self.__dict__.items()]
        ) + "\n)"

    def __repr__(self):
        return self.__str__()


def is_dict_field_missing(value, field_name):
    """Check if a specific field in a value is None or empty."""
    return value.get(field_name) in [None, "", {}, [], ()]


def get_days_between_dates(date1, date2):
    # Convert the date strings to datetime objects
    datetime1 = datetime.strptime(date1, "%Y%m%d").date()
    datetime2 = datetime.strptime(date2, "%Y%m%d").date()

    # Calculate the number of days between the two dates
    num_days = abs((datetime2 - datetime1).days)
    return num_days


def find_base_directory():
    current_file = os.path.abspath(__file__)
    base_directory = os.path.dirname(current_file)
    return base_directory


def load_json_file(path):
    try:
        with open(path) as file:
            loaded_dict = json.load(file)
        return loaded_dict
    except FileNotFoundError as e:
        raise e
    except json.JSONDecodeError as e:
        raise e
    except Exception as e:
        raise e


def run_terminal_command(command):
    try:
        # Run the command and capture the output
        result = subprocess.run(command, shell=True, universal_newlines=True, check=False)
        # print('Terminal Command Results:', result)
        # INFO(f'Terminal Command: {result.args} ')
        # INFO(f'Terminal Command Results: {result.returncode} ')

        return result.returncode
    except subprocess.CalledProcessError as e:
        raise e
    except Exception as e:
        raise e


# TODO: this should be moved to become a class instead
def recursive_op_files(source, destination, source_pattern, override=False, skip_dir=True, operation='copy'):
    files_count = 0
    try:
        assert source is not None, 'Please specify source path, Current source is None.'
        assert destination is not None, 'Please specify destination path, Current source is None.'

        if not os.path.exists(destination):
            print(f'Creating Dir: {destination}')
            os.mkdir(destination)

        items = glob.glob(os.path.join(source, source_pattern))

        for item in items:

            try:
                if os.path.isdir(item) and not skip_dir:
                    path = os.path.join(destination, os.path.basename(item))
                    # INFO(f'START {operation} FROM {item} TO {path}.')
                    files_count += recursive_op_files(
                        source=item, destination=path,
                        source_pattern=source_pattern, override=override
                    )
                else:
                    file = os.path.join(destination, os.path.basename(item))
                    print(f'START {operation} FROM {item} TO {file}.')
                    if not os.path.exists(file) or override:
                        if operation == 'copy':
                            shutil.copyfile(item, file)
                        elif operation == 'move':
                            shutil.move(item, file)
                        else:
                            raise ValueError(f"Invalid operation: {operation}")
                        files_count += 1
                    else:
                        raise FileExistsError(f'The file {file} already exists int the destination path {destination}.')
            except FileNotFoundError as e_file:
                print(f"File not found error: {e_file}")
            except PermissionError as e_permission:
                print(f"Permission error: {e_permission}")
            except Exception as e_inner:
                print(f"An error occurred: {e_inner}")
    except AssertionError as e_assert:
        print(f"Assertion error: {e_assert}")
    except Exception as e_outer:
        print(f"An error occurred: {e_outer}")
    return files_count


def convert_to_json(items):
    x = json.dumps(items)
    print(x)
