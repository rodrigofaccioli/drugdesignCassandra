"""
    Removes separator_filename_mode from file name.
    Example:
    separator_filename_mode : +----+
    file_name ns3pro_dm_0_-_NuBBE_102_obabel_3D+----+1
    return ns3pro_dm_0_-_NuBBE_102_obabel_3D
"""
def remover_separator_filename_mode(separator_filename_mode, file_name):
    return str(str(file_name).split(separator_filename_mode)[0]).strip()
