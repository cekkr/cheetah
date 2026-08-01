from __future__ import annotations

import sys
import unittest

from cheetah_db.server import DEFAULT_BINARY, server_binary_name


class ServerBinaryNameTests(unittest.TestCase):
    def test_windows_uses_an_executable_suffix(self) -> None:
        self.assertEqual(server_binary_name("win32"), "cheetah-server.exe")
        self.assertEqual(server_binary_name("linux"), "cheetah-server")
        self.assertEqual(server_binary_name("darwin"), "cheetah-server")

    def test_default_binary_follows_the_current_platform(self) -> None:
        self.assertEqual(DEFAULT_BINARY.name, server_binary_name(sys.platform))


if __name__ == "__main__":
    unittest.main()
