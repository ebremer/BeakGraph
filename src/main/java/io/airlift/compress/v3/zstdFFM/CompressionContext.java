/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.v3.zstdFFM;

import static io.airlift.compress.v3.zstdFFM.Constants.MAX_BLOCK_SIZE;
import static io.airlift.compress.v3.zstdFFM.Util.checkArgument;
import static java.lang.Math.clamp;

class CompressionContext
{
    public final CompressionParameters parameters;
    public final RepeatedOffsets offsets = new RepeatedOffsets();
    public final BlockCompressionState blockCompressionState;
    public final SequenceStore sequenceStore;

    public final SequenceEncodingContext sequenceEncodingContext = new SequenceEncodingContext();

    public final HuffmanCompressionContext huffmanContext = new HuffmanCompressionContext();

    public CompressionContext(CompressionParameters parameters, long baseAddress, int inputSize)
    {
        this.parameters = parameters;

        int windowSize = clamp(inputSize, 1, parameters.getWindowSize());
        int blockSize = Math.min(MAX_BLOCK_SIZE, windowSize);
        int divider = (parameters.getSearchLength() == 3) ? 3 : 4;

        int maxSequences = blockSize / divider;

        sequenceStore = new SequenceStore(blockSize, maxSequences);

        blockCompressionState = new BlockCompressionState(parameters, baseAddress);
    }

    public void slideWindow(int slideWindowSize)
    {
        checkArgument(slideWindowSize > 0, "slideWindowSize must be positive");
        blockCompressionState.slideWindow(slideWindowSize);
    }

    public void commit()
    {
        offsets.commit();
        huffmanContext.saveChanges();
    }

    /**
     * Makes this context indistinguishable from a freshly constructed one for a new
     * frame starting at {@code baseAddress}: repeat offsets, hash/chain tables and
     * window, Huffman tables and the sequence store all restart. The sizes stay as
     * constructed, so the caller must have built it for at least these parameters
     * (BeakGraph divergence: ZstdJavaCompressor keeps one context per window size
     * instead of allocating the tables and workspaces for every frame, see README.md).
     */
    public void reset(long baseAddress)
    {
        offsets.reset();
        blockCompressionState.reset(baseAddress);
        huffmanContext.reset();
        sequenceStore.reset();
    }
}
