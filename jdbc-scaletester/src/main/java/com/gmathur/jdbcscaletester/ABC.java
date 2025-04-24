package com.gmathur.jdbcscaletester;

public class ABC {
}
//ATCG generation of variations

//ATCG
//AGCT
//GCTA
//GATC
//..
//..


// ABCAD
// AADCB
//        0 1 2
//           root              level 0
//      |             |       |
//       A B C          B A C  C B A     level 0/i = 0
//       |   |
//   A B C   A C B                 level 1/i=1
// |
//

//                             level 2/i=2
//

import java.util.*;

public class Main {
    // Helper routine to swap two characters in an array
    static private void swap(char[] basePairs, int i, int j) {
        char tmp = basePairs[i];
        basePairs[i] = basePairs[j];
        basePairs[j] = tmp;
    }


    static private List<String> res = new LinkedList<>();

    static private void _generate(char[] basePairs, int i, Stack<String> resultVec) {
        if (i == basePairs.length) {
            Iterator<String> it = resultVec.iterator();
            while (it.hasNext()) {
                String ele = it.next();
                res.add(ele);
            }
            return;
        }

        for (int j = i; j < basePairs.length; j++) {
            swap(basePairs, i, j);
            resultVec.push(new String(basePairs));

            _generate(basePairs, i+1, resultVec);

            resultVec.pop();
            swap(basePairs, i, j);
        }

        return;
    }

    static String[] generate(String basePairs) {
        // DS to gather generated variations
        Stack<String> resultStack = new Stack<>();
        _generate(basePairs.toCharArray(), 0, resultStack);

        // Convert result vector to an array of Strings
        int L = res.size();
        String[] r = new String[L];
        for (int i = 0; i < L; i++) {
            r[i] = res.get(i);
        }

        return r;
    }

    public static void main(String[] args) {
        String input = "ATCG";

        String[] result = generate(input);

        System.out.println("Total generation: " + result.length);
        System.out.println("Results:");

        for (String r: result) {
            System.out.println(r);
        }
    }
}