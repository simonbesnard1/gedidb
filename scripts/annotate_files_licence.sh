#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
# SPDX-License-Identifier: EUPL-1.2
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Simon Besnard
# Contact :   ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de

Institutions="Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences"
authors=("Amelia Holcomb" "Felix Dombrowski" "Simon Besnard")
Contact="ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de"
year=2024
license_python="EUPL-1.2"
version="2.0"

shopt -s globstar

for file in $(find . -type f -name "*.py"); 
do
    # Remove existing SPDX headers to avoid duplication
    sed -i '/^# SPDX-/d' "$file"
    sed -i '/^# Contact/d' "$file"
    sed -i '/^# Version/d' "$file"

    # Add the custom SPDX header
    echo "# SPDX-FileCopyrightText: $year $Institutions" | cat - "$file" > temp && mv temp "$file"
    
    for author in "${authors[@]}"; do
        echo "# SPDX-FileCopyrightText: $year $author" | cat - "$file" > temp && mv temp "$file"
    done

    echo "# Contact: $Contact" | cat - "$file" > temp && mv temp "$file"
    echo "# Version: $version" | cat - "$file" > temp && mv temp "$file"
    echo "# SPDX-License-Identifier: $license_python" | cat - "$file" > temp && mv temp "$file"
done

