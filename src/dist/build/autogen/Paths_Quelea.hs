module Paths_Quelea (
    version,
    getBinDir, getLibDir, getDataDir, getLibexecDir,
    getDataFileName
  ) where

import qualified Control.Exception as Exception
import Data.Version (Version(..))
import System.Environment (getEnv)
import Prelude

catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
catchIO = Exception.catch


version :: Version
version = Version {versionBranch = [0,0,1], versionTags = []}
bindir, libdir, datadir, libexecdir :: FilePath

bindir     = "/home/parallels/.cabal/bin"
libdir     = "/home/parallels/.cabal/lib/Quelea-0.0.1/ghc-7.6.3"
datadir    = "/home/parallels/.cabal/share/Quelea-0.0.1"
libexecdir = "/home/parallels/.cabal/libexec"

getBinDir, getLibDir, getDataDir, getLibexecDir :: IO FilePath
getBinDir = catchIO (getEnv "Quelea_bindir") (\_ -> return bindir)
getLibDir = catchIO (getEnv "Quelea_libdir") (\_ -> return libdir)
getDataDir = catchIO (getEnv "Quelea_datadir") (\_ -> return datadir)
getLibexecDir = catchIO (getEnv "Quelea_libexecdir") (\_ -> return libexecdir)

getDataFileName :: FilePath -> IO FilePath
getDataFileName name = do
  dir <- getDataDir
  return (dir ++ "/" ++ name)