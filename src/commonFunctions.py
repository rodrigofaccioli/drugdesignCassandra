"""
    Removes separator_filename_mode from file name.
    Example:
    separator_filename_mode : +----+
    file_name ns3pro_dm_0_-_NuBBE_102_obabel_3D+----+1
    return ns3pro_dm_0_-_NuBBE_102_obabel_3D
"""
def remover_separator_filename_mode(separator_filename_mode, file_name):
    return str(str(file_name).split(separator_filename_mode)[0]).strip()

"""
    Removes separator_filename_mode from file name.
    Example:
    separator_receptor : _-_
    separator_filename_mode : +----+
    file_name ns3pro_dm_0_-_NuBBE_102_obabel_3D+----+1
    return NuBBE_102_obabel_3D
"""
def get_molecule_name_from_filename_mode(separator_receptor, separator_filename_mode, file_name):
    aux = remover_separator_filename_mode(separator_filename_mode, file_name)
    aux = str(str(aux).split(separator_receptor)[1]).strip()
    return aux
