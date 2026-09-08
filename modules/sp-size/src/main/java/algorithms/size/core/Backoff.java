package algorithms.size.core;

/**
 *  This is an implementation of the paper "Concurrent Size" by Gal Sela and Erez Petrank.
 *
 *  Copyright (C) 2022  Gal Sela
 *  Contact Gal Sela (sela.galy@gmail.com) with any questions or comments.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

public class Backoff {
    private int backoffAmount = 1;
    public int dummyCounter = 0;

    public void backoff() {
        for (int i = 0; i < backoffAmount; i++)
            dummyCounter += 1;
    }

    public void increase() {
        int backoff = backoffAmount * 2;
        if (backoff > 512) backoff = 512;
        backoffAmount = backoff;
    }

    public void decrease() {
        int backoff = backoffAmount / 2;
        if (backoff == 0) backoff = 1;
        backoffAmount = backoff;
    }
}
