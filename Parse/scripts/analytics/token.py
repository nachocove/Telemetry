class Token:
    @staticmethod
    def assert_token(token):
        if not isinstance(token, Token):
            raise TypeError(token.__class__)

    def __init__(self, s=None):
        self.string = s

    def __eq__(self, other):
        if self.string is None or other.string is None:
            return True
        return self.string == other.string

    def __ne__(self, other):
        return not (self == other)

    def is_wildcard(self):
        return self.string is None

    def __str__(self):
        if self.string is None:
            return '[...]'
        return self.string


class TokenList:
    def __init__(self, tokens, ref_obj=None):
        assert isinstance(tokens, list)
        self.tokens = tokens
        self.ref_obj = ref_obj

    def __len__(self):
        return len(self.tokens)

    def __sub__(self, other):
        if len(self) != len(other):
            raise ValueError('TokenLists of different lengths (%d, %d)' % (len(self), len(other)))
        return sum([(lambda a, b: a != b and 1 or 0)(x, y) for (x, y) in zip(self.tokens, other.tokens)])

    def __str__(self):
        return ' '.join([unicode(x) for x in self.tokens])


class Tokenizer:
    def __init__(self):
        pass

    def process(self, content):
        raise NotImplementedError()


class WhiteSpaceTokenizer(Tokenizer):
    def __init__(self):
        Tokenizer.__init__(self)

    def process(self, content):
        return [Token(x) for x in content.split()]


class TokenIndex:
    def __init__(self, tokens):
        self._tokens = dict()
        idx = 0
        for token in tokens:
            if token in self.tokens:
                self._tokens[token].append(idx)
            else:
                self._tokens[token] = [idx]
            idx += 1

    def tokens(self):
        return self.tokens.keys()

    def has_token(self, token):
        return token in self._tokens
