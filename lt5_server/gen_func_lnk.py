
from os.path import normcase

def splitunc(p):
    """Split a pathname into UNC mount point and relative path specifiers.

    Return a 2-tuple (unc, rest); either part may be empty.
    If unc is not empty, it has the form '//host/mount' (or similar
    using backslashes).  unc+rest is always the input path.
    Paths containing drive letters never have an UNC part.
    """
    if p[1:2] == ':':
        return '', p # Drive letter present
    firstTwo = p[0:2]
    if firstTwo == '//' or firstTwo == '\\\\':
        # is a UNC path:
        # vvvvvvvvvvvvvvvvvvvv equivalent to drive letter
        # \\machine\mountpoint\directories...
        #           directory ^^^^^^^^^^^^^^^
        normp = normcase(p)
        index = normp.find('\\', 2)
        if index == -1:
            ##raise RuntimeError, 'illegal UNC path: "' + p + '"'
            return ("", p)
        index = normp.find('\\', index + 1)
        if index == -1:
            index = len(p)
        return p[:index], p[index:]
    return '', p
