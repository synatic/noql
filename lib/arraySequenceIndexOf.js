/**
 * Finds the starting index of the first occurrence of a sequence within an array, using a custom comparison function.
 * @param {Array} sequenceArr - The sequence of elements to search for.
 * @param {Array} inArray - The array in which to search for the sequence.
 * @param {number} startIndex - the index to start searching from
 * @param {Function} [compareFn] - Optional custom comparison function that takes two arguments and returns a boolean indicating whether they are equal.
 * @returns {number} The starting index of the first occurrence of the sequence in the array or -1 if the sequence is not found.
 */
function arraySequenceIndexOf(sequenceArr, inArray, startIndex = 0, compareFn) {
    compareFn =
        compareFn ||
        function (a, b) {
            return a === b;
        };
    if (sequenceArr.length === 0) return 0;
    if (sequenceArr.length === 1) {
        return inArray.findIndex((a) => compareFn(a, sequenceArr[0]));
    }
    if (sequenceArr.length > inArray.length - startIndex) return -1;

    let i = startIndex;
    while (i < inArray.length) {
        if (!compareFn(sequenceArr[0], inArray[i])) {
            i++;
            continue;
        }
        if (inArray.length < i + sequenceArr.length) {
            return -1;
        }
        let j = 0;
        let seqTrue = true;
        while (seqTrue && j < sequenceArr.length) {
            seqTrue = seqTrue && compareFn(sequenceArr[j], inArray[i + j]);
            j++;
        }
        if (seqTrue) {
            return i;
        }

        i++;
    }

    return -1;
}

module.exports = arraySequenceIndexOf;
