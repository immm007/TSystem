# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""


def get_change_percentage(actual, prev):
    if actual is None or prev is None or prev == 0:
        raise Exception("Invalid values")

    diff = actual-prev
    ret = diff / float(abs(prev))
    return ret


def safe_min(left, right):
    if left is None:
        return right
    elif right is None:
        return left
    else:
        return min(left, right)


def safe_max(left, right):
    if left is None:
        return right
    elif right is None:
        return left
    else:
        return max(left, right)

def addPrefix(code,sh,sz):
    if code[0] == '6':
        return sh + code
    elif code[0] == '0' or code[0] == '3':
        return sz + code
    raise RuntimeError('unsupoorted code %s' % code)

def addSinaPrefix(code):
    return addPrefix(code,'sh','sz')

def addWangyiPrefix(code):
    return addPrefix(code,'0','1')


class CSVStringHelper:
    '''
    为了提高解析下载的CSV字符串效率的类
    '''

    def __init__(self, s):
        self.__s = s
        self.__len = len(s)
        self.__newLinePos = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.__newLinePos == self.__len:
            raise StopIteration
        startPos = self.__newLinePos
        while self.__s[self.__newLinePos] != '\n':
            self.__newLinePos += 1
        self.__newLinePos += 1
        return self.__s[startPos:self.__newLinePos - 1]
