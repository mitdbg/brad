# Copyright 2022 The Balsa Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re

def _CanonicalizeJoinCond(join_cond):
    """join_cond: 4-tuple"""
    t1, c1, t2, c2 = join_cond
    if t1 < t2:
        return join_cond
    return t2, c2, t1, c1


def _DedupJoinConds(join_conds):
    """join_conds: list of 4-tuple (t1, c1, t2, c2)."""
    canonical_join_conds = [_CanonicalizeJoinCond(jc) for jc in join_conds]
    return sorted(set(canonical_join_conds))


def _GetJoinConds(sql):
    """Returns a list of join conditions in the form of (t1, c1, t2, c2)."""
    quotation = False
    join_cond_pat = re.compile(
        r"""
        (\w+)  # 1st table
        \.     # the dot "."
        (\w+)  # 1st table column
        \s*    # optional whitespace
        =      # the equal sign "="
        \s*    # optional whitespace
        (\w+)  # 2nd table
        \.     # the dot "."
        (\w+)  # 2nd table column
        """, re.VERBOSE)
    join_conds = join_cond_pat.findall(sql)
    if len(join_conds) == 0:
        join_cond_pat = re.compile(
            r"""
            \"
            (\w+)  # 1st table
            \"
            \.     # the dot "."
            \"
            (\w+)  # 1st table column
            \"
            \s*    # optional whitespace
            =      # the equal sign "="
            \s*    # optional whitespace
            \"
            (\w+)  # 2nd table
            \"
            \.     # the dot "."
            \"
            (\w+)  # 2nd table column
            \"
            """, re.VERBOSE)
        join_conds = join_cond_pat.findall(sql)
        quotation = True
        return _DedupJoinConds(join_conds), quotation
    return _DedupJoinConds(join_conds), quotation


def _FormatJoinCond(tup, quotation=False):
    t1, c1, t2, c2 = tup
    if quotation:
        return f"\"{t1}\".\"{c1}\" = \"{t2}\".\"{c2}\""
    else:
        return f"{t1}.{c1} = {t2}.{c2}"
