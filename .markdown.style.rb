# frozen_string_literal: true

# Style rule for my personal markdown style preferences for mdl
# https://github.com/mivok/markdownlint/blob/master/docs/creating_styles.md

# Start by including all rules
all

#
# MD031:
#
# https://github.com/mivok/markdownlint/blob/master/docs/\
#    RULES.md#md031---fenced-code-blocks-should-be-surrounded-by-blank-lines
#
# Cannot figure out how to meet this rule - doesn't seem to work
# properly--I have blank lines before and after these blocks!
#
# e.g., this triggers:
#
# 5. Test away!
#
#   ```
#   ../deploy/scripts/db cms-${stack:?}-dbadmin
#   ```
### Backups
exclude_rule 'MD031'

#
# MD029:
#
# Reconfigure rule MD029 so that ordered lists don't have to always be
# labeled as '1', but instead must proceed from 1 to 2 to 3 to ...
#
rule 'MD029', style: 'ordered'

#
# MD013
#
# Reconfigure rule MD013 so that we don't check for long line length
# within a code block, as having a big long one-liner is sometimes
# better than trusting a person will get multiple lines copied
# correctly.
#
rule 'MD013', code_blocks: false
