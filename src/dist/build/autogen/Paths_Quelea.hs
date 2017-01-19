module Paths_Quelea (
    version,
    getBinDir, getLibDir, getDataDir, getLibexecDir,
    getDataFileName, getSysconfDir
  ) where

import qualified Control.Exception as Exception
import Data.Version (Version(..))
import System.Environment (getEnv)
import Prelude

catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
catchIO = Exception.catch

version :: Version
version = Version [0,0,1] []
bindir, libdir, datadir, libexecdir, sysconfdir :: FilePath

bindir     = "/home/parallels/.cabal/bin"
libdir     = "/home/parallels/.cabal/lib/x86_64-linux-ghc-7.10.3/Quelea-0.0.1-4QJnBQOT8OVJw77Qn1mnmm"
datadir    = "/home/parallels/.cabal/share/x86_64-linux-ghc-7.10.3/Quelea-0.0.1"
libexecdir = "/home/parallels/.cabal/libexec"
sysconfdir = "/home/parallels/.cabal/etc"

getBinDir, getLibDir, getDataDir, getLibexecDir, getSysconfDir :: IO FilePath
getBinDir = catchIO (getEnv "Quelea_bindir") (\_ -> return bindir)
getLibDir = catchIO (getEnv "Quelea_libdir") (\_ -> return libdir)
getDataDir = catchIO (getEnv "Quelea_datadir") (\_ -> return datadir)
getLibexecDir = catchIO (getEnv "Quelea_libexecdir") (\_ -> return libexecdir)
getSysconfDir = catchIO (getEnv "Quelea_sysconfdir") (\_ -> return sysconfdir)

getDataFileName :: FilePath -> IO FilePath
getDataFileName name = do
  dir <- getDataDir
  return (dir ++ "/" ++ name)
