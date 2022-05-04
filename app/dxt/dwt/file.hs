{-# LANGUAGE LinearTypes #-}
{-#  LANGUAGE ScopedTypeVariables  #-}
{-#  LANGUAGE PatternSynonyms      #-}
{-#  LANGUAGE FlexibleContexts     #-}
{-#  OPTIONS_GHC -Wno-overlapping-patterns #-}

module DWT.Manifile
    ( Manifile(..)
    , Mani1d(..)
    , Mani2d(..)
    , Mani3d(..)
    , unMani1d
    , unMani2d
    , unMani3d
    , readManiByteString
    , writeManiByteString
    , writeManifile
    , loadManifile
    , readManiByteString1D
    , readManiByteString2D
    , readManiByteString3D
    , loadManifile1D
    , loadManifile2D
    , loadManifile3D
    )
where

import qualified Data.Massiv.Array as A
import Data.Massiv.Array ( Array
                         , S
                         , Ix1(..)
                         , Ix2(..)
                         , Ix3(..)
                         , Sz
                         , Sz1(..)
                         , Sz2(..)
                         , Sz3(..)
                         , pattern Sz1
                         , pattern Sz2
                         , pattern Sz3
                         )
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Vector.Storable as VU
import Control.Monad.Trans.Class ()

import qualified Data.Binary     as B
import qualified Data.Binary.Put as B
import qualified Data.Binary.Get as B
import Data.Int ( Int32, Int64 )

import Manipipe.Util ( padR )
import Manipipe.Futhark.Type
import Manipipe.Futhark.ShapeList
    ( shapeListElementCount, ShapeList(..) )
import Manipipe.Csv ()
import Control.Lens (none)
--import Codec.Compression.Lzma (compressIO, decompressIO) --Uncomment for FileCompression -- Manifile Implements a LZMA Algorithm to accomplish Lossless Compression(!! NO GZIP OR BZIP)
--import Data.ByteString.Base64 (encode decode) -- Uncomment for Base64 String Support

newtype Manifile sh = Mani {
    unMani :: Array S sh Float
}

type Mani1d = Manifile Ix1
type Mani2d = Manifile Ix2
type Mani3d = Manifile Ix3

--The First 14 Bytes of the file are this dataType
data ManifileHeader
    = Compressed Int
    | Shape Int
    | Dim0  Int
    | Dim1  Int
    | Dim2  Int
    | Dim3  Int
    deriving(Show, Eq)

unMani1d :: Manifile Ix1 -> Array S Ix1 Float
unMani2d :: Manifile Ix2 -> Array S Ix2 Float
unMani3d :: Manifile Ix3 -> Array S Ix3 Float
unMani1d = unMani
unMani2d = unMani
unMani3d = unMani

toInt32 :: Integral t => t -> Int32
toInt32 = fromIntegral

fromInt32 :: Integral t => Int32 -> t
fromInt32 = fromIntegral

toInt64 :: Integral t => t -> Int64
toInt64 = fromIntegral

fromInt64 :: Integral t => Int64 -> t
fromInt64 = fromIntegral

doN :: Monad m => m x -> Int -> m [x]
doN f i =
    if i > 0
    then do x <- f
            xs <- doN f (i-1)
            return (x:xs)
    else return []

instance ( A.Index ix
         , ShapeList ix)
         => B.Binary (Manifile ix) where
    put (Mani arr) =
        let shapeList = shapeToList (A.size arr)
            dim       = toInt32 $ length shapeList
            padList   = map toInt32 $ padR 4 0 shapeList
        in
        do  B.put (0::Int32)
            B.put dim
            mapM_ B.put padList
            mapM_ B.put (A.toList arr)
    get =
        do  _   <- B.get :: B.Get Int32
            dim <- B.get :: B.Get Int32
            parts <- take (fromInt32 dim) . map fromInt32 <$> doN (B.get :: B.Get Int32) 4
            list <- doN (B.get :: B.Get Float) (shapeListElementCount parts)
            let shape :: A.Sz ix = listToShape parts
            return . Mani . A.resize' shape . A.fromList A.Par $ list

foldMani :: Semigroup (Array S sh Float) => Manifile sh -> Manifile sh -> Manifile sh
foldMani x y = Mani (unMani x <> unMani y)

readManiByteString :: forall ix
                   .  ( A.Index ix
                      , ShapeList ix
                      )
                   => BL.ByteString
                   -> Manifile ix
readManiByteString = B.runGet (B.get :: B.Get (Manifile ix))

readManiByteString1D :: BL.ByteString -> Mani1d
readManiByteString1D = readManiByteString

readManiByteString2D :: BL.ByteString -> Mani2d
readManiByteString2D = readManiByteString

readManiByteString3D :: BL.ByteString -> Mani3d
readManiByteString3D = readManiByteString

writeManiByteString :: forall ix
                    .  ( A.Index ix
                       , ShapeList ix
                       )
                    => Manifile ix
                    -> BL.ByteString
writeManiByteString mani =
    B.runPut (B.put mani)

writeManifile :: forall ix
               . ( A.Index ix
                 , ShapeList ix
                 )
              => FilePath
              -> Manifile ix
              -> IO ()
writeManifile path mani =
    BL.writeFile (path ++ ".mani") (writeManiByteString mani)

loadManifile :: forall ix
             .  ( A.Index ix
                , ShapeList ix
                )
             => FilePath
             -> IO (Manifile ix)
loadManifile path =
  do bs <- BL.readFile path
     return $ readManiByteString bs

loadManifile1D :: FilePath
               -> IO (Manifile A.Ix1)
loadManifile1D = loadManifile

loadManifile2D :: FilePath
               -> IO (Manifile A.Ix2)
loadManifile2D = loadManifile

loadManifile3D :: FilePath
               -> IO (Manifile A.Ix3)
loadManifile3D = loadManifile

writeEncodedManifile :: forall ix
               . ( A.Index ix
                 , ShapeList ix
                 )
              => FilePath
              -> Manifile ix
              -> IO ()
writeEncodedManifile path mani =
    BL.writeFile (path ++ ".mani") (encode writeManiByteString mani)

writeCompressedManifile :: forall ix
               . ( A.Index ix
                 , ShapeList ix
                 )
              => FilePath
              -> Manifile ix
              -> IO ()
writeCompressedManifile path mani =
    BL.writeFile (path ++ ".mani") (compressIO writeManiByteString mani)


loadAndDecodeManifile :: forall ix
             .  ( A.Index ix
                , ShapeList ix
                )
             => FilePath
             -> A.Sz ix
             -> IO (Manifile ix)
loadAndDecodeManifile path sh =
  do bs <- decodeLenient BL.readFile path
     return $ readManiByteString bs

loadAndDecompressManiFile  :: forall ix
             .  ( A.Index ix
                , ShapeList ix
                )
             => FilePath
             -> A.Sz ix
             -> IO (Manifile ix)
loadAndDecompressManiFile path sh =
  do bs <- decompressIO BL.readFile path
     return $ readManiByteString bs

linearLoadManiFile ::forall ix . (A.Index ix, ShapeList ix) => FilePath -> Sz ix %1 -> Control.IO(Manifile ix)
linearLoadManiFile path sh =
    handle1 <- Linear.openFile path Linear.WriteMode
    handle2 <- BL.writeFile (path ++ ".mani") (writeManiByteString handle1) --Linear.hPutStrLn handle1 (Text.pack "Hello Manifold")
    (path) <- Linear.hClose handle2
    Control.return (Ur (path))

-- instance Foldable Manifile where
--    foldMap f empty = mempty
--    foldMap f x = f . unMani x --fmap(unMani x ) .  f --intersperse f x

-- intersperse :: (a -> m) -> Array S a Float -> m
-- intersperse a b = do
--     let x = unMani a <> b
--     return x

