package com.wibmo.trident.compress.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.xerial.snappy.Snappy;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4CompressorWithLength;
import net.jpountz.lz4.LZ4DecompressorWithLength;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) {

		try {
			App app = new App();
			app.compress();
		}
		catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		catch (DataFormatException e) {
			e.printStackTrace();
		}
	}

	private void compress() throws DataFormatException, IOException {
		System.out.println("Starting Compression Process...");
		//while (true) {
			for (int i = 1; i <= 3; i++) {
				byte[] fileBytes = readFile(i);
				System.out.println("\n==================================================\n");
				System.out.println("File " + i);
				System.out.println("Data Size Before: " + fileBytes.length);
				//compressByLZ4(ByteBuffer.wrap(fileBytes));
				compressByLZ42(ByteBuffer.wrap(fileBytes));
				//compressBySnappy(ByteBuffer.wrap(fileBytes), fileBytes.length);
				//compressByJavaZip(fileBytes, fileBytes.length);
			}
		//}
	}

	private void compressByLZ4(ByteBuffer fileBytes) throws IOException {
		LZ4Factory factory = LZ4Factory.fastestInstance();
		
		System.out.println("\n>>> LZ4 Compression >>>");
		LZ4Compressor compressor = factory.fastCompressor();
		ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
		
		int maxCompressedLength = compressor.maxCompressedLength(fileBytes.remaining());
		long startTime1 = System.currentTimeMillis();
		LZ4BlockOutputStream lzbos = new LZ4BlockOutputStream(compressedOutput, maxCompressedLength, compressor);
		lzbos.write(fileBytes.array());
		lzbos.close();
		long endTime1 = System.currentTimeMillis();
		System.out.println("Time Taken for Compression: " + (endTime1 - startTime1));
		
		System.out.println("Data Compressed: " + maxCompressedLength);
		System.out.println("Data Size After: " + compressedOutput.toByteArray().length);
		
		System.out.println("\n>>> LZ4 Decompression >>>");
		LZ4FastDecompressor decompressor = factory.fastDecompressor();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		long startTime2 = System.currentTimeMillis();
		LZ4BlockInputStream lzbis = new LZ4BlockInputStream(new ByteArrayInputStream(compressedOutput.toByteArray()), decompressor);
		int count;
		byte[] buffer = new byte[4096];
		while ((count = lzbis.read(buffer)) != -1) {
			baos.write(buffer, 0, count);
		}
		lzbis.close();
		long endTime2 = System.currentTimeMillis();
		System.out.println("Time Taken for Decompression: " + (endTime2 - startTime2));
		
		System.out.println("Data Bytes read for Decompression: " + compressedOutput.toByteArray().length);
		System.out.println("Data Size After Decompression: " + baos.toByteArray().length);
	}
	
	private void compressByLZ42(ByteBuffer fileBytes) throws IOException {
		LZ4Factory factory = LZ4Factory.fastestInstance();
		
		System.out.println("\n>>> LZ4 Compression >>>");
		LZ4Compressor compressor = factory.fastCompressor();
		LZ4CompressorWithLength compressorWrapper = new LZ4CompressorWithLength(compressor);
		int maxCompressedLength = compressorWrapper.maxCompressedLength(fileBytes.remaining());
		ByteBuffer compressed = ByteBuffer.allocate(maxCompressedLength);
		
		long startTime1 = System.currentTimeMillis();
		compressed = ByteBuffer.wrap(compressorWrapper.compress(fileBytes.array()));
		long endTime1 = System.currentTimeMillis();
		System.out.println("Time Taken for Compression: " + (endTime1 - startTime1));
		
		System.out.println("Data Compressed: " + maxCompressedLength);
		System.out.println("Data Size After: " + compressed.remaining());
		
		System.out.println("\n>>> LZ4 Decompression >>>");
		LZ4FastDecompressor decompressor = factory.fastDecompressor();
		LZ4DecompressorWithLength decompressorWrapper = new LZ4DecompressorWithLength(decompressor);
		int maxDecompressedLength = LZ4DecompressorWithLength.getDecompressedLength(compressed.array());
		ByteBuffer decompressed = ByteBuffer.allocate(maxDecompressedLength);

		long startTime2 = System.currentTimeMillis();
		decompressed = ByteBuffer.wrap(decompressorWrapper.decompress(compressed.array()));
		long endTime2 = System.currentTimeMillis();
		System.out.println("Time Taken for Decompression: " + (endTime2 - startTime2));
		
		System.out.println("Data Bytes read for Decompression: " + maxDecompressedLength);
		System.out.println("Data Size After Decompression: " + decompressed.remaining());
	}
	
	private void compressBySnappy(ByteBuffer fileBytes, final int decompressedLength) throws IOException {
		System.out.println("\n>>> Snappy Compression >>>");
		ByteBuffer compressed = ByteBuffer.allocate(Snappy.maxCompressedLength(fileBytes.remaining()));
		
		long startTime1 = System.currentTimeMillis();
		int compressedLength = Snappy.compress(fileBytes.array(), fileBytes.position(), fileBytes.remaining(), compressed.array(), 0);
		long endTime1 = System.currentTimeMillis();
		System.out.println("Time Taken for Compression: " + (endTime1 - startTime1));
		
		compressed.limit(compressedLength);
				 
		System.out.println("Data Compressed: " + compressedLength);
		System.out.println("Data Size After: " + compressed.remaining());
		
		System.out.println("\n>>> Snappy Decompression >>>");
		ByteBuffer decompressed = ByteBuffer.allocate(Snappy.uncompressedLength(compressed.array()));
		//byte[] toBeRestored = new byte[1];
		
		long startTime2 = System.currentTimeMillis();
		int restoredDataLength = Snappy.uncompress(compressed.array(), compressed.position(), compressed.remaining(), decompressed.array(), 0);
		long endTime2 = System.currentTimeMillis();
		System.out.println("Time Taken for Decompression: " + (endTime2 - startTime2));
		
		System.out.println("Data Bytes read for Decompression: " + restoredDataLength);
		System.out.println("Data Size After Decompression: " + decompressed.remaining());
	}
	
	private void compressByJavaZip(byte[] fileBytes, final int decompressedLength) throws DataFormatException {
		System.out.println("\n>>> Java ZIP Compression >>>");
		Deflater compressor = new Deflater();
		compressor.setLevel(Deflater.BEST_COMPRESSION);
		compressor.setInput(fileBytes);
		compressor.finish();
		
		int compressedLength = 0;
		ByteArrayOutputStream bos = new ByteArrayOutputStream(fileBytes.length);
		byte[] compressed = new byte[1024];
		while (!compressor.finished()) {
			compressedLength = compressor.deflate(compressed);
			bos.write(compressed, 0, compressedLength);
		}
		System.out.println("Data Size After: " + compressedLength);
		
		System.out.println("\n>>> Java ZIP Decompression >>>");
		Inflater decompresser = new Inflater();
		//byte[] toBeRestored = new byte[1];
		decompresser.setInput(compressed);
		
		int restoredDataLength = 0;
		ByteArrayOutputStream debos = new ByteArrayOutputStream(compressed.length);
		byte[] decompressed = new byte[1024];
		while (!decompresser.finished()) {
			restoredDataLength = decompresser.inflate(decompressed);
			debos.write(decompressed, 0, restoredDataLength);
		}
		System.out.println("Data Bytes Read For Decompression: " + restoredDataLength);
		System.out.println("Data Size After Decompression: " + decompressed.length);
	}

	private byte[] readFile(int index) throws IOException {
		byte[] result = null;
		File file = new File("E:\\WORKSPACE\\Sample_" + index + ".txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		try {
			String str = null;
			StringBuffer sb = new StringBuffer();
			while ((str = br.readLine()) != null) {
				sb.append(str);
			}
			//System.out.println("File Content: " + sb);
			result = sb.toString().getBytes();
			//System.out.println("File Size: " + result.length);
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
		finally {
			br.close();
		}
		return result;
	}
}
