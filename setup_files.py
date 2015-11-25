import threading
import traceback

__author__ = 'constantinm'

import os
import shutil
import sys
import argparse
import random
from time import sleep
from bing import Bing, KEY

FILE_CONTENT = """
    Lorem ipsum dolor sit amet, consectetur adipiscing elit. Ut egestas condimentum egestas.
    Vestibulum ut facilisis neque, eu finibus mi. Proin ac massa sapien. Sed mollis posuere erat vel malesuada.
    Nulla non dictum nulla. Quisque eu porttitor leo. Nunc auctor vitae risus non dapibus. Integer rhoncus laoreet varius.
    Donec pulvinar dapibus finibus. Suspendisse vitae diam quam. Morbi tincidunt arcu nec ultrices consequat.
    Nunc ornare turpis pellentesque augue laoreet, non sollicitudin lectus aliquam.
    Sed posuere vel arcu ut elementum. In dictum commodo nibh et blandit. Vivamus sed enim sem.
    Nunc interdum rhoncus eros gravida vestibulum. Suspendisse sit amet feugiat mauris, eget tristique est.
    Ut efficitur mauris quis tortor laoreet semper. Pellentesque eu tincidunt tortor, malesuada rutrum massa.
    Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos.
    Duis gravida, turpis at pulvinar dictum, arcu lacus dapibus nisl, eget luctus metus sapien id turpis.
    Donec consequat gravida diam at bibendum. Vivamus tincidunt congue nisi, quis finibus eros tincidunt nec.
    Aenean ut leo non nulla sodales dapibus. Quisque sit amet vestibulum urna.
    Vivamus imperdiet sed elit eu aliquam. Maecenas a ultrices diam. Praesent dapibus interdum orci pellentesque tempor.
    Morbi a luctus dui. Integer nec risus sit amet turpis varius lobortis. Vestibulum at ligula ut purus vestibulum pharetra.
    Fusce est libero, tristique in magna sed, ullamcorper consectetur justo. Aliquam erat volutpat.
    Mauris sollicitudin neque sit amet augue congue, a ornare mi iaculis. Praesent vestibulum laoreet urna, at sodales
    velit cursus iaculis.
    Sed quis enim hendrerit, viverra leo placerat, vestibulum nulla. Vestibulum ligula nisi, semper et cursus eu, gravida at enim.
    Vestibulum vel auctor augue. Aliquam pulvinar diam at nunc efficitur accumsan. Proin eu sodales quam.
    Quisque consectetur euismod mauris, vel efficitur lorem placerat ac. Integer facilisis non felis ut posuere.
    Vestibulum vitae nisi vel odio vehicula luctus. Nunc sagittis eu risus sed feugiat.
    Nunc magna dui, auctor id luctus vel, gravida eget sapien. Donec commodo, risus et tristique hendrerit, est tortor
    molestie ex, in tristique dui augue vel mauris. Nam sagittis diam sit amet sapien fermentum, quis congue tellus venenatis.
    Donec facilisis diam eget elit tempus, ut tristique mi congue. Ut ut consectetur ex. Ut non tortor eleifend,
    feugiat felis et, pretium quam. Pellentesque at orci in lorem placerat tincidunt eget quis purus.
    Donec orci odio, luctus ut sagittis nec, congue sit amet ex. Donec arcu diam, fermentum ac porttitor consectetur,
    blandit et diam. Vivamus efficitur erat nec justo vestibulum fringilla. Mauris quis dictum elit, eget tempus ex.
    """

wait_for = 0.01
after_each = 10


class ItemError(Exception):
    pass


def create_files(target, name_prefix='file', content=FILE_CONTENT, num_created=1, verbose=False, delay=False):
    if not num_created or int(num_created) <= 0:
        return
    # create target directory if it does not exist
    if not os.path.exists(target):
        os.makedirs(target)
    name_pattern, count = make_file_pattern(name_prefix, num_created)

    if verbose:
        print 'created files'

    index = 0
    global num_operations
    for i in range(count):
        try:
            with open(os.path.join(target, name_pattern % i), 'w') as f:
                f.write(content)
            index += 1
            num_operations += 1
            if verbose:
                print_file_details(name_pattern % i)
            if delay:
                wait()
        except:
            pass
    print 'created %d files' % index


def create_files_from_internet(target, query, file_types=[Bing.TXT_FILE_TYPE], num_created=1, verbose=False):
    count = int(num_created)
    if not num_created or count <= 0:
        return
    # create target directory if it does not exist
    if not os.path.exists(target):
        os.makedirs(target)

    if verbose:
        print 'created files'

    bing = Bing(KEY)
    files_per_type = count / len(file_types)
    remainder = count - files_per_type * len(file_types)
    file_tuples = []
    for ft in file_types:
        num = files_per_type + 1 if remainder > 0 else files_per_type
        file_tuples.append((ft, num))
        remainder -= 1

    display = print_file_details if verbose else None
    total = bing.execute(target, query, file_tuples, display=display)
    if verbose:
        print 'created %d files' % total


def create_directory_with_files(target, dir_prefix='folder', file_prefix='file', content=FILE_CONTENT,
                                levels=1, num_dir_per_level_created=1, num_files_per_dir_created=1,
                                verbose=False, delay=False):
    if not levels or int(levels) <= 0:
        return
    if not num_dir_per_level_created or int(num_dir_per_level_created) <= 0:
        return
    if not num_files_per_dir_created or int(num_files_per_dir_created) <= 0:
        return
    # create target directory if it does not exist
    if not os.path.exists(target):
        os.makedirs(target)

    # make the naming pattern for file
    file_name_pattern, files_count_per_dir, total_files = make_file_pattern(file_prefix, '.txt',
                                                                            levels, num_dir_per_level_created,
                                                                            num_files_per_dir_created)
    # make the naming pattern for directory
    dir_name_pattern, dirs_count_per_level, total_dirs = make_dir_pattern(dir_prefix, levels, num_dir_per_level_created)

    if verbose:
        print 'creating directories (%d) and files (%d)' % (total_dirs, total_files)

    # recursive function to build each level of directories and files
    def create_level(current_target, _num_levels, _current_level, _num_dirs_per_level, _num_files_per_dir,
                     _current_dirs_count, _current_files_count, show_details=None):
        if _current_level <= 0:
            return _current_dirs_count, _current_files_count

        _dirs_count = _current_dirs_count
        _files_count = _current_files_count

        for i in range(_num_dirs_per_level):
            dir_name = dir_name_pattern % (_num_levels - _current_level, i)
            dir_path = os.path.join(current_target, dir_name)
            # create directory
            try:
                os.makedirs(dir_path)
                _dirs_count += 1
                # num_operations += 1
                if show_details:
                    show_details(dir_name)
                # recurse into the next level
                _dirs_count, _files_count = create_level(dir_path, _num_levels, _current_level - 1, _num_dirs_per_level,
                                                         _num_files_per_dir, _dirs_count, _files_count,
                                                         show_details=show_details)
            except:
                pass

            for j in range(_num_files_per_dir):
                # create file
                try:
                    filename = file_name_pattern % (_num_levels - _current_level, i, j)
                    with open(os.path.join(dir_path, filename), 'w') as f:
                        f.write(content)
                    _files_count += 1
                    # num_operations += 1
                    if show_details:
                        show_details(filename, indent_level=2)
                    if delay:
                        wait()
                except:
                    pass
        return _dirs_count, _files_count

    show_details = print_file_details if verbose else None
    d, f = create_level(target, levels, levels, num_dir_per_level_created, num_files_per_dir_created, 0, 0,
                        show_details=show_details)

    global num_operations
    num_operations = d + f

    if verbose:
        print 'created %d directories with %d files' % (d, f)


def create_directory_with_files_from_internet(target, query, file_types=[Bing.TXT_FILE_TYPE], dir_prefix='folder',
                                              levels=1, num_dir_per_level_created=1, num_files_per_dir_created=1,
                                              verbose=False, delay=False):
    if not levels or int(levels) <= 0:
        return
    if not num_dir_per_level_created or int(num_dir_per_level_created) <= 0:
        return
    if not num_files_per_dir_created or int(num_files_per_dir_created) <= 0:
        return
    # create target directory if it does not exist
    if not os.path.exists(target):
        os.makedirs(target)

    # make the naming pattern for file
    _, files_count_per_dir, total_files = make_file_pattern('', '', levels, num_dir_per_level_created,
                                                            num_files_per_dir_created)
    # make the naming pattern for directory
    dir_name_pattern, dirs_count_per_level, total_dirs = make_dir_pattern(dir_prefix, levels, num_dir_per_level_created)

    if verbose:
        print 'creating directories (%d) and files (%d)' % (total_dirs, total_files)

    # recursive function to build each level of directories and files
    def create_level(current_target, urls, from_to_list, _num_levels, _current_level, _num_dirs_per_level,
                     _num_files_per_dir, _current_dirs_count, show_details=None):

        _dirs_count = _current_dirs_count
        if _current_level <= 0:
            return _dirs_count

        for i in range(_num_dirs_per_level):
            dir_name = dir_name_pattern % (_num_levels - _current_level, i)
            dir_path = os.path.join(current_target, dir_name)
            # create directory
            try:
                os.makedirs(dir_path)
                _dirs_count += 1
                # num_operations += 1
                if show_details:
                    show_details(dir_name)

                # prepare the files download list
                src_list = []
                for i in range(_num_files_per_dir):
                    src_list.append(urls.pop(0))
                from_to_list.append({'src': src_list, 'dst': dir_path})

                # recurse into the next level
                _dirs_count = create_level(dir_path, urls, from_to_list, _num_levels, _current_level - 1,
                                           _num_dirs_per_level, _num_files_per_dir, _dirs_count,
                                           show_details=show_details)
            except:
                pass

        return _dirs_count

    bing = Bing(KEY)
    urls = []
    files_per_type = total_files / len(file_types)
    remainder = total_files - files_per_type * len(file_types)
    file_tuples = []
    for ft in file_types:
        num = files_per_type + 1 if remainder > 0 else files_per_type
        file_tuples.append((ft, num))
        remainder -= 1

    result_total = 0
    for file_tuple in file_tuples:
        result_per_file_type = 0
        for url in bing.get_files(query, file_tuple[0], file_tuple[1]):
            urls.append(url)
            result_per_file_type += 1
        print 'found %d (%d) results for %s type' % (result_per_file_type, file_tuple[1], file_tuple[0])
        result_total += result_per_file_type
    print 'found %d results for querying "%s"' % (result_total, query)

    show_details = print_file_details if verbose else None
    # create the directory structure and prepare list of files to download
    from_to_list = []
    d = create_level(target, urls, from_to_list, levels, levels, num_dir_per_level_created,
                                   num_files_per_dir_created, 0, show_details=show_details)

    show_download_details = print_file_download_details if verbose else None
    # parallel downloading of files in their respctive directories
    f = bing.execute2(from_to_list, display=show_download_details)

    global num_operations
    num_operations = d + f

    if verbose:
        print 'created %d directories with %d files' % (d, f)


def make_file_pattern(name_prefix, extension, num_levels, num_dirs_per_level, num_files_per_dir):
    count_levels = int(num_levels)
    count_dirs_per_level = int(num_dirs_per_level)
    count_files_per_dir = int(num_files_per_dir)
    _, dir_num_digits, count_dirs = make_dir_pattern('', num_levels, num_dirs_per_level)
    # file_num_digits = len(str(count_files_per_dir - 1)) + dir_num_digits
    total_count_files = count_dirs * count_files_per_dir
    name_pattern = name_prefix + '%%0%dd%%0%dd%%0%dd%s' % \
                                 (len(str(count_levels - 1)), len(str(count_dirs_per_level - 1)),
                                  len(str(count_files_per_dir - 1)), extension)
    return name_pattern, count_files_per_dir, total_count_files


def make_dir_pattern(name_prefix, num_levels, num_dirs_per_level):
    count_levels = int(num_levels)
    count_dirs_per_level = int(num_dirs_per_level)
    total_count_dirs = (count_dirs_per_level ** (count_levels + 1) - 1) / (count_dirs_per_level - 1) - 1
    # format for directory numbering is 'llnn' where 'll' is the level number and 'nn' is the directory number,
    # e.g. if num_levels=2 and num_dirs=10, directory will be numbered as '00', '01',..., '09', '10', '11',...,'19
    # dir_num_digits = len(str(count_levels-1)) + count_levels * len(str(count_dirs_per_level-1))
    name_pattern = name_prefix + '%%0%dd%%0%dd' % (len(str(count_levels - 1)), len(str(count_dirs_per_level - 1)))
    return name_pattern, count_dirs_per_level, total_count_dirs


def wait():
    if num_operations % after_each == 0:
        sleep(wait_for)


def print_file_details(filename, indent_level=1):
    def indent(tab_level):
        for i in range(tab_level):
            print '\t',

    indent(indent_level)
    print 'created file %s' % filename


def print_file_download_details(from_to, indent_level=1):
    def indent(tab_level):
        for i in range(tab_level):
            print '\t',

    indent(indent_level)
    url = from_to[0]
    path = from_to[1]
    print 'downloaded from %s to %s' % (url, path)


def get_children(dir_to_flatten, include_dirs=False, exclude_dirs=None):
    """
    return a dictionary of all children of the given directory
    :param dir_to_flatten: the "root" directory for which to return the children
    :param include_dirs: true to include strict descendent directories in the return
    :return: dictionary. The keys are the full path of the child file (or directory), and the value is the number of
             its own children (a file has 0 children)
    """

    def get_children_for_level(files, count, dir_to_flatten, include_dirs=include_dirs, exclude_dirs=exclude_dirs,
                               root=False):
        for dirpath, dirnames, filenames in os.walk(dir_to_flatten):
            for filename in filenames:
                files[os.path.join(dirpath, filename)] = 0
                count += 1
            if include_dirs:
                if dirnames:
                    for dirname in dirnames:
                        subdirpath = os.path.join(dir_to_flatten, dirname)
                        if exclude_dirs and subdirpath in exclude_dirs:
                            continue
                        count += 1
                        count1 = 0
                        count1 = get_children_for_level(files, count1, subdirpath,
                                                        include_dirs=include_dirs, exclude_dirs=exclude_dirs)
                        files[subdirpath] = count1
                        count += count1
                else:
                    if not root:
                        files[dir_to_flatten] = 0
                break
        return count

    files = {}
    count = 0
    count = get_children_for_level(files, count, dir_to_flatten, include_dirs=include_dirs, exclude_dirs=exclude_dirs,
                                   root=True)
    return count, files


def get_num_children(dir_to_flatten, include_dirs=False, exclude_dirs=None):
    """
    return the number of children for the given directory
    :param dir_to_flatten: the "root" directory for which to calculate the number of children
    :param include_dirs: true to include strict descendent directories in the count
    :return: number of children
    """
    count = 0
    for dirpath, dirnames, filenames in os.walk(dir_to_flatten):
        count += len(filenames)
        if include_dirs:
            if exclude_dirs:
                dirnames = [d for d in dirnames if os.path.join(dirpath, d) not in exclude_dirs]
            count += len(dirnames)
    return count


def delete_files(target, num_deleted=1, verbose=False, recursive=False, delay=False):
    """

    :param target: parent directory
    :param num_deleted: number of files to be deleted
    :param verbose: show path for each deleted file
    :param recursive: true to recurse in subdirectories
    :param delay: add some delay between file delete operations
    :return:
    """
    if not num_deleted or int(num_deleted) <= 0:
        return

    count = int(num_deleted)
    children_dict = get_children(target, include_dirs=recursive)
    if recursive:
        total = get_num_children(target, include_dirs=recursive)
        children = children_dict.keys()
    else:
        children = os.listdir(target)
        total = len(children)
        children = [os.path.join(target, child) for child in children]

    if total == 0:
        # nothing to delete
        return

    if verbose:
        print 'deleted files'

    random.seed()
    index = 0
    while index < count:
        filepath = children[random.randint(0, total - 1)]
        try:
            if os.path.isfile(filepath) or recursive and os.path.isdir(filepath):
                if os.path.exists(filepath):
                    os.remove(filepath)
                else:
                    raise ItemError()
                deleted = 1
                if os.path.isdir(filepath):
                    deleted += children_dict[filepath]
                index += deleted
                global num_operations
                num_operations += 1
                if verbose:
                    print_file_details(filepath)
                if delay:
                    wait()
            else:
                raise ItemError()
        except OSError as e:
            # some files/directories are read-only, skip
            print 'error removing %s: %s' % (filepath, str(e))
        except ItemError as e:
            if not os.listdir(target):
                # directory is empty
                break

    print 'deleted %d files' % index


def rename_files(target, num_renamed=1, verbose=False, recursive=False, delay=False):
    """
    Rename a subset of files by adding the suffix '__1' to the flename (before extension).
    :param target: parent directory
    :param num_renamed: number of files to be renamed
    :param verbose: show path for each renamed file
    :param recursive: true to recurse in subdirectories
    :return: nothing
    """
    if not num_renamed or int(num_renamed) <= 0:
        return

    count = int(num_renamed)
    children_dict = get_children(target, include_dirs=recursive)
    if recursive:
        total = get_num_children(target, include_dirs=recursive)
        children = children_dict.keys()
    else:
        children = os.listdir(target)
        total = len(children)
        children = [os.oath.join(target, child) for child in children]

    if total == 0:
        # nothing to rename
        return
    if verbose:
        print 'renamed files'

    random.seed()
    index = 0
    while index < count:
        filepath = children[random.randint(0, total - 1)]
        path_no_ext, ext = os.path.splitext(filepath)
        try:
            if os.path.isfile(filepath) or recursive and os.path.isdir(filepath):
                if os.path.exists(filepath):
                    os.rename(filepath, path_no_ext + '__1' + ext)
                else:
                    raise ItemError()
                renamed = 1
                if os.path.isdir(filepath):
                    renamed += children_dict[filepath]
                index += renamed
                global num_operations
                num_operations += 1
                if verbose:
                    print_file_details(filepath)
                if delay:
                    wait()
            else:
                raise ItemError()
        except OSError as e:
            print 'error renaming %s: %s' % (filepath, str(e))
        except ItemError as e:
            if not os.path.listdir(target):
                # directory is empty
                break

    print 'renamed %d files' % index


def move_files(target, subfolder=None, num_moved=1, verbose=False, recursive=False, delay=False):
    """
    Move a subset of files to a subfolder.
    :param target: parent directory
    :param subfolder: name of the subfolder only, relative to target, to move the files into
    :param num_moved: number of files to be moved
    :param verbose: show path for each moved file
    :param recursive: true to recurse in subdirectories
    :return: nothing
    """
    if not num_moved or int(num_moved) <= 0:
        return

    count = int(num_moved)

    if not subfolder:
        subfolder = os.path.basename(target) + '__1'
    subfolder = os.path.join(target, subfolder)
    if not os.path.exists(subfolder):
        os.makedirs(subfolder)

    children_dict = get_children(target, include_dirs=recursive, exclude_dirs=[subfolder])
    if recursive:
        # total = get_num_children(target, include_dirs=recursive, exclude_dirs=[subfolder])
        total = children_dict[0]
        children = children_dict[1].keys()
    else:
        children = os.listdir(target)
        total = len(children)
        children = [os.oath.join(target, child) for child in children]

    if total == 0:
        # nothing to move
        return
    if verbose:
        print 'moved files'

    index = 0
    random.seed()
    while index < count:
        filepath = children[random.randint(0, total - 1)]
        try:
            if os.path.isfile(filepath) or recursive and os.path.isdir(filepath):
                if os.path.exists(filepath):
                    shutil.move(filepath, subfolder)
                else:
                    raise ItemError('error moving %s->%s' % (filepath, subfolder))
                moved = 1
                if os.path.isdir(filepath):
                    moved += children_dict[1][filepath]
                index += moved
                global num_operations
                num_operations += 1
                if verbose:
                    print_file_details(filepath)
                if delay:
                    wait()
            else:
                raise ItemError('%s was already moved' % filepath)
        except OSError as e:
            # some directories are read-only, skip
            print 'error moving %s: %s' % (filepath, str(e))
        except ItemError as e:
            print 'error moving item: %s' % str(e)
            if not os.listdir(target):
                # directory is empty
                break

    print 'moved %d files' % index


class DelayAction(argparse.Action):
    def __init__(self, option_strings, nargs='*', default=False, **kwargs):
        if option_strings != ['--add_delay', '-delay']:
            raise ValueError('DelayAction action currently applies only to the --add_delay/-delay option')

        if nargs != '*':
            raise ValueError('nargs should be 0 or more arguments')

        if default is True:
            raise ValueError('default should be false')

        super(DelayAction, self).__init__(option_strings, nargs=nargs, default=default, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string in self.option_strings:
            setattr(namespace, self.dest, True)
        wait_for, after_each = DelayAction._get_delay_args(values)
        setattr(namespace, 'wait_for', wait_for)
        setattr(namespace, 'after_each', after_each)

    @staticmethod
    def _get_delay_args(args):
        global wait_for, after_each
        _after_each = after_each
        _wait_for = wait_for
        if type(args) == list:
            if len(args) == 0:
                pass
            elif len(args) == 2:
                try:
                    wait_for = float(args[0])
                except ValueError:
                    raise ValueError(
                        '%s is not a valid "wait time" value for the delay option (must be between 0.001 and 5 [sec])'
                        % args[0])
                try:
                    after_each = float(args[1])
                except ValueError:
                    raise ValueError(
                        '%s is not a valid "after number of operation" value for the delay option (must be between 1 and 1000)'
                        % args[1])
            else:
                raise ValueError('only 0 or 2 arguments are allowed for the delay option')
        return wait_for, after_each


class CreateFullDirAction(argparse.Action):
    def __init__(self, option_strings, nargs='*', **kwargs):
        if option_strings != ['--number-full-dir-created', '-nfdc']:
            raise ValueError(
                'CreateFullDirAction action currently applies only to the --number-full-dir-created/-nfdc option')

        if nargs != '*':
            raise ValueError('nargs is ignored')

        super(CreateFullDirAction, self).__init__(option_strings, nargs=nargs, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string in self.option_strings:
            setattr(namespace, self.dest, True)
        num_dir_levels, num_dir_created, num_files_per_dir_created = CreateFullDirAction._get_create_dir_with_files_args(
            values)
        setattr(namespace, 'num_dir_levels', num_dir_levels)
        setattr(namespace, 'num_dir_created', num_dir_created)
        setattr(namespace, 'num_files_per_dir_created', num_files_per_dir_created)

    @staticmethod
    def _get_create_dir_with_files_args(args):
        num_dir_levels = 0
        num_dir_created = 0
        num_files_per_dir_created = 0

        try:
            if len(args) != 1:
                raise ValueError(
                    'argument value for --number-full-dir-created/-nfdc option shoudld be in the l/m/n format')
            values = args[0].split('/')
            if len(values) != 3:
                raise ValueError(
                    'argument value for --number-full-dir-created/-nfdc option shoudld be in the l/m/n format')
            try:
                num_dir_levels = int(values[0])
                num_dir_created = int(values[1])
                num_files_per_dir_created = int(values[2])
            except ValueError as e:
                raise e
        except ValueError as e:
            raise e

        return num_dir_levels, num_dir_created, num_files_per_dir_created


def create_parser():
    parser = argparse.ArgumentParser(add_help=True,
                                     description=
                                     'create a number of files with identical content in the target directory')
    parser.add_argument('--target-dir', '-d',
                        default=os.getcwd(),
                        help='target directory')
    parser.add_argument('--file-content', '-c',
                        default=FILE_CONTENT,
                        help='content of the text file')
    parser.add_argument('--file-content-query', '-cq',
                        help='a query for selecting files to download, e.g. "document management"')
    parser.add_argument('--file-types', '-ft',
                        nargs='*',
                        choices=['PDF', 'IMAGE', 'TXT', 'TEXT', 'DOC', 'PPT', 'HTM', 'HTML', 'RTF', 'XLS'],
                        help='list file types to download, e.g. PDF, TXT, IMAGE, DOC, etc.')
    # parser.add_argument('--number-query-results', '-nqr',
    #                     default=0,
    #                     help='number of query results, i.e. number of files to download')
    parser.add_argument('--file-prefix', '-fp',
                        default='file',
                        help='prefix for the filename')
    parser.add_argument('--dir-prefix', '-dp',
                        default='folder',
                        help='prefix for the directory')
    parser.add_argument('--number-files-created', '-nfc',
                        default=0,
                        help='number of files to create')
    parser.add_argument('--number-empty-dir-created', '-nedc',
                        default=0,
                        help='number of empty directories to create')
    parser.add_argument('--number-full-dir-created', '-nfdc',
                        action=CreateFullDirAction,
                        help='number of directories with files to create')
    parser.add_argument('--number-files-deleted', '-nfd',
                        default=0,
                        help='number of files to delete')
    parser.add_argument('--number-files-renamed', '-nfr',
                        default=0,
                        help='number of files to rename')
    parser.add_argument('--number-files-moved', '-nfm',
                        default=0,
                        help='number of files to move')
    parser.add_argument('--list-created', '-lc',
                        action='store_true',
                        help='list the created files')
    parser.add_argument('--list-deleted', '-ld',
                        action='store_true',
                        help='list the deleted files')
    parser.add_argument('--list-renamed', '-lr',
                        action='store_true',
                        help='list the renamed files')
    parser.add_argument('--list-moved', '-lm',
                        action='store_true',
                        help='list the moved files')
    parser.add_argument('--add_delay', '-delay',
                        # nargs='*',
                        # default=False,
                        action=DelayAction,
                        help=
                        '''
                        Add some delay to files manipulation.
                        Optionally, add delay amount (e.g. 0.1 means 100ms), and how often
                        (e.g. 20 means every 20 file operations).
                        Defaults are 10ms every 10 operations.
                        ''')
    parser.add_argument('--recursive', '-r',
                        action='store_true',
                        help='apply operations like delete, rename or move recursively')

    return parser


def win32_unicode_argv():
    """Uses shell32.GetCommandLineArgvW to get sys.argv as a list of Unicode
    strings.

    Versions 2.x of Python don't support Unicode in sys.argv on
    Windows, with the underlying Windows API instead replacing multi-byte
    characters with '?'.

    See http://stackoverflow.com/questions/846850/read-unicode-characters-from-command-line-arguments-in-python-2-x-on-windows
    """

    from ctypes import POINTER, byref, cdll, c_int, windll
    from ctypes.wintypes import LPCWSTR, LPWSTR

    GetCommandLineW = cdll.kernel32.GetCommandLineW
    GetCommandLineW.argtypes = []
    GetCommandLineW.restype = LPCWSTR

    CommandLineToArgvW = windll.shell32.CommandLineToArgvW
    CommandLineToArgvW.argtypes = [LPCWSTR, POINTER(c_int)]
    CommandLineToArgvW.restype = POINTER(LPWSTR)

    cmd = GetCommandLineW()
    argc = c_int(0)
    argv = CommandLineToArgvW(cmd, byref(argc))
    if argc.value > 0:
        # Remove Python executable and commands if present
        start = argc.value - len(sys.argv)
        return [argv[i] for i in
                xrange(start, argc.value)]


def dumpstacks(signal, frame):
    id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    for thread_id, stack in sys._current_frames().items():
        code.append("\n# Thread: %s(%d)" % (id2name.get(thread_id, ""),
                                            thread_id))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s'
                        % (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
    print "\n".join(code)


def main(argv=None):
    # Print thread dump when receiving SIGUSR1,
    # except under Windows (no SIGUSR1)
    import signal
    # Get the Ctrl+C to interrupt application
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if sys.platform != 'win32':
        signal.signal(signal.SIGUSR1, dumpstacks)
    if argv is None:
        argv = win32_unicode_argv() if sys.platform == 'win32' else sys.argv

    global num_operations
    num_operations = 0
    parser = create_parser()
    options = parser.parse_args(argv[1:])
    target = options.target_dir
    if not os.path.isabs(target):
        target = os.path.abspath(target)

    delay = options.add_delay
    if delay:
        global wait_for, after_each
        wait_for = options.wait_for
        after_each = options.after_each

    # must be called in this order?
    # default for files created is 0
    if options.number_files_created:
        if options.file_content_query:
            create_files_from_internet(target, options.file_content_query, file_types=options.file_types,
                                       num_created=options.number_files_created, verbose=options.list_created)
        else:
            create_files(target, name_prefix=options.file_prefix, content=options.file_content,
                         num_created=options.number_files_created, verbose=options.list_created, delay=delay)
    # TODO create empty directories

    if options.number_full_dir_created:
        if options.file_content_query:
            create_directory_with_files_from_internet(target, options.file_content_query, dir_prefix=options.dir_prefix,
                                                      file_types=options.file_types, levels=options.num_dir_levels,
                                                      num_dir_per_level_created=options.num_dir_created,
                                                      num_files_per_dir_created=options.num_files_per_dir_created,
                                                      verbose=options.list_created, delay=delay)
        else:
            create_directory_with_files(target, dir_prefix=options.dir_prefix, file_prefix=options.file_prefix,
                                        content=options.file_content, levels=options.num_dir_levels,
                                        num_dir_per_level_created=options.num_dir_created,
                                        num_files_per_dir_created=options.num_files_per_dir_created,
                                        verbose=options.list_created, delay=delay)
    # default for files deleted is 0
    delete_files(target, num_deleted=options.number_files_deleted, verbose=options.list_deleted,
                 delay=options.add_delay, recursive=options.recursive)
    # default for files renamed is 0
    rename_files(target, num_renamed=options.number_files_renamed, verbose=options.list_renamed,
                 delay=options.add_delay, recursive=options.recursive)
    # default for files moved is 0
    move_files(target, num_moved=options.number_files_moved, verbose=options.list_moved,
               delay=options.add_delay, recursive=options.recursive)


if __name__ == "__main__":
    sys.exit(main())
