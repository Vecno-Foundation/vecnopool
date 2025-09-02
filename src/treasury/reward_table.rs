use anyhow::{Result, anyhow};
use log::debug;

pub fn get_block_reward(daa_score: u64) -> Result<u64> {
    if daa_score == 0 {
        return Err(anyhow!("Invalid daa_score: 0"));
    }

    let reward_ve = if daa_score < 2_629_800 {
        debug!("Using reward of 2.0 VE for daa_score {} < 2629800", daa_score);
        2.0
    } else if daa_score < 5_259_600 {
        debug!("Using reward of 1.943063882 VE for daa_score {} < 5259600", daa_score);
        1.943063882
    } else if daa_score < 7_889_400 {
        debug!("Using reward of 1.887748625 VE for daa_score {} < 7889400", daa_score);
        1.887748625
    } else if daa_score < 10_519_200 {
        debug!("Using reward of 1.834008086 VE for daa_score {} < 10519200", daa_score);
        1.834008086
    } else if daa_score < 13_149_000 {
        debug!("Using reward of 1.781797436 VE for daa_score {} < 13149000", daa_score);
        1.781797436
    } else if daa_score < 15_778_800 {
        debug!("Using reward of 1.731073122 VE for daa_score {} < 15778800", daa_score);
        1.731073122
    } else if daa_score < 18_408_600 {
        debug!("Using reward of 1.681792831 VE for daa_score {} < 18408600", daa_score);
        1.681792831
    } else if daa_score < 21_038_400 {
        debug!("Using reward of 1.633915453 VE for daa_score {} < 21038400", daa_score);
        1.633915453
    } else if daa_score < 23_668_200 {
        debug!("Using reward of 1.587401052 VE for daa_score {} < 23668200", daa_score);
        1.587401052
    } else if daa_score < 26_298_000 {
        debug!("Using reward of 1.542210825 VE for daa_score {} < 26298000", daa_score);
        1.542210825
    } else if daa_score < 28_927_800 {
        debug!("Using reward of 1.498307077 VE for daa_score {} < 28927800", daa_score);
        1.498307077
    } else if daa_score < 31_557_600 {
        debug!("Using reward of 1.455653183 VE for daa_score {} < 31557600", daa_score);
        1.455653183
    } else if daa_score < 34_187_400 {
        debug!("Using reward of 1.414213562 VE for daa_score {} < 34187400", daa_score);
        1.414213562
    } else if daa_score < 36_817_200 {
        debug!("Using reward of 1.373953647 VE for daa_score {} < 36817200", daa_score);
        1.373953647
    } else if daa_score < 39_447_000 {
        debug!("Using reward of 1.334839854 VE for daa_score {} < 39447000", daa_score);
        1.334839854
    } else if daa_score < 42_076_800 {
        debug!("Using reward of 1.296839555 VE for daa_score {} < 42076800", daa_score);
        1.296839555
    } else if daa_score < 44_706_600 {
        debug!("Using reward of 1.25992105 VE for daa_score {} < 44706600", daa_score);
        1.25992105
    } else if daa_score < 47_336_400 {
        debug!("Using reward of 1.224053543 VE for daa_score {} < 47336400", daa_score);
        1.224053543
    } else if daa_score < 49_966_200 {
        debug!("Using reward of 1.189207115 VE for daa_score {} < 49966200", daa_score);
        1.189207115
    } else if daa_score < 52_596_000 {
        debug!("Using reward of 1.155352697 VE for daa_score {} < 52596000", daa_score);
        1.155352697
    } else if daa_score < 55_225_800 {
        debug!("Using reward of 1.122462048 VE for daa_score {} < 55225800", daa_score);
        1.122462048
    } else if daa_score < 57_855_600 {
        debug!("Using reward of 1.090507733 VE for daa_score {} < 57855600", daa_score);
        1.090507733
    } else if daa_score < 60_485_400 {
        debug!("Using reward of 1.059463094 VE for daa_score {} < 60485400", daa_score);
        1.059463094
    } else if daa_score < 63_115_200 {
        debug!("Using reward of 1.029302237 VE for daa_score {} < 63115200", daa_score);
        1.029302237
    } else if daa_score < 65_745_000 {
        debug!("Using reward of 1.0 VE for daa_score {} < 65745000", daa_score);
        1.0
    } else if daa_score < 68_374_800 {
        debug!("Using reward of 0.971531941 VE for daa_score {} < 68374800", daa_score);
        0.971531941
    } else if daa_score < 71_004_600 {
        debug!("Using reward of 0.943874313 VE for daa_score {} < 71004600", daa_score);
        0.943874313
    } else if daa_score < 73_634_400 {
        debug!("Using reward of 0.917004043 VE for daa_score {} < 73634400", daa_score);
        0.917004043
    } else if daa_score < 76_264_200 {
        debug!("Using reward of 0.890898718 VE for daa_score {} < 76264200", daa_score);
        0.890898718
    } else if daa_score < 78_894_000 {
        debug!("Using reward of 0.865536561 VE for daa_score {} < 78894000", daa_score);
        0.865536561
    } else if daa_score < 81_523_800 {
        debug!("Using reward of 0.840896415 VE for daa_score {} < 81523800", daa_score);
        0.840896415
    } else if daa_score < 84_153_600 {
        debug!("Using reward of 0.816957727 VE for daa_score {} < 84153600", daa_score);
        0.816957727
    } else if daa_score < 86_783_400 {
        debug!("Using reward of 0.793700526 VE for daa_score {} < 86783400", daa_score);
        0.793700526
    } else if daa_score < 89_413_200 {
        debug!("Using reward of 0.771105413 VE for daa_score {} < 89413200", daa_score);
        0.771105413
    } else if daa_score < 92_043_000 {
        debug!("Using reward of 0.749153538 VE for daa_score {} < 92043000", daa_score);
        0.749153538
    } else if daa_score < 94_672_800 {
        debug!("Using reward of 0.727826591 VE for daa_score {} < 94672800", daa_score);
        0.727826591
    } else if daa_score < 97_302_600 {
        debug!("Using reward of 0.707106781 VE for daa_score {} < 97302600", daa_score);
        0.707106781
    } else if daa_score < 99_932_400 {
        debug!("Using reward of 0.686976824 VE for daa_score {} < 99932400", daa_score);
        0.686976824
    } else if daa_score < 102_562_200 {
        debug!("Using reward of 0.667419927 VE for daa_score {} < 102562200", daa_score);
        0.667419927
    } else if daa_score < 105_192_000 {
        debug!("Using reward of 0.648419777 VE for daa_score {} < 105192000", daa_score);
        0.648419777
    } else if daa_score < 107_821_800 {
        debug!("Using reward of 0.629960525 VE for daa_score {} < 107821800", daa_score);
        0.629960525
    } else if daa_score < 110_451_600 {
        debug!("Using reward of 0.612026772 VE for daa_score {} < 110451600", daa_score);
        0.612026772
    } else if daa_score < 113_081_400 {
        debug!("Using reward of 0.594603558 VE for daa_score {} < 113081400", daa_score);
        0.594603558
    } else if daa_score < 115_711_200 {
        debug!("Using reward of 0.577676348 VE for daa_score {} < 115711200", daa_score);
        0.577676348
    } else if daa_score < 118_341_000 {
        debug!("Using reward of 0.561231024 VE for daa_score {} < 118341000", daa_score);
        0.561231024
    } else if daa_score < 120_970_800 {
        debug!("Using reward of 0.545253866 VE for daa_score {} < 120970800", daa_score);
        0.545253866
    } else if daa_score < 123_600_600 {
        debug!("Using reward of 0.529731547 VE for daa_score {} < 123600600", daa_score);
        0.529731547
    } else if daa_score < 126_230_400 {
        debug!("Using reward of 0.514651118 VE for daa_score {} < 126230400", daa_score);
        0.514651118
    } else if daa_score < 128_860_200 {
        debug!("Using reward of 0.5 VE for daa_score {} < 128860200", daa_score);
        0.5
    } else if daa_score < 131_490_000 {
        debug!("Using reward of 0.485765971 VE for daa_score {} < 131490000", daa_score);
        0.485765971
    } else if daa_score < 134_119_800 {
        debug!("Using reward of 0.471937156 VE for daa_score {} < 134119800", daa_score);
        0.471937156
    } else if daa_score < 136_749_600 {
        debug!("Using reward of 0.458502022 VE for daa_score {} < 136749600", daa_score);
        0.458502022
    } else if daa_score < 139_379_400 {
        debug!("Using reward of 0.445449359 VE for daa_score {} < 139379400", daa_score);
        0.445449359
    } else if daa_score < 142_009_200 {
        debug!("Using reward of 0.432768281 VE for daa_score {} < 142009200", daa_score);
        0.432768281
    } else if daa_score < 144_639_000 {
        debug!("Using reward of 0.420448208 VE for daa_score {} < 144639000", daa_score);
        0.420448208
    } else if daa_score < 147_268_800 {
        debug!("Using reward of 0.408478863 VE for daa_score {} < 147268800", daa_score);
        0.408478863
    } else if daa_score < 149_898_600 {
        debug!("Using reward of 0.396850263 VE for daa_score {} < 149898600", daa_score);
        0.396850263
    } else if daa_score < 152_528_400 {
        debug!("Using reward of 0.385552706 VE for daa_score {} < 152528400", daa_score);
        0.385552706
    } else if daa_score < 155_158_200 {
        debug!("Using reward of 0.374576769 VE for daa_score {} < 155158200", daa_score);
        0.374576769
    } else if daa_score < 157_788_000 {
        debug!("Using reward of 0.363913296 VE for daa_score {} < 157788000", daa_score);
        0.363913296
    } else if daa_score < 160_417_800 {
        debug!("Using reward of 0.353553391 VE for daa_score {} < 160417800", daa_score);
        0.353553391
    } else if daa_score < 163_047_600 {
        debug!("Using reward of 0.343488412 VE for daa_score {} < 163047600", daa_score);
        0.343488412
    } else if daa_score < 165_677_400 {
        debug!("Using reward of 0.333709964 VE for daa_score {} < 165677400", daa_score);
        0.333709964
    } else if daa_score < 168_307_200 {
        debug!("Using reward of 0.324209889 VE for daa_score {} < 168307200", daa_score);
        0.324209889
    } else if daa_score < 170_937_000 {
        debug!("Using reward of 0.314980262 VE for daa_score {} < 170937000", daa_score);
        0.314980262
    } else if daa_score < 173_566_800 {
        debug!("Using reward of 0.306013386 VE for daa_score {} < 173566800", daa_score);
        0.306013386
    } else if daa_score < 176_196_600 {
        debug!("Using reward of 0.297301779 VE for daa_score {} < 176196600", daa_score);
        0.297301779
    } else if daa_score < 178_826_400 {
        debug!("Using reward of 0.288838174 VE for daa_score {} < 178826400", daa_score);
        0.288838174
    } else if daa_score < 181_456_200 {
        debug!("Using reward of 0.280615512 VE for daa_score {} < 181456200", daa_score);
        0.280615512
    } else if daa_score < 184_086_000 {
        debug!("Using reward of 0.272626933 VE for daa_score {} < 184086000", daa_score);
        0.272626933
    } else if daa_score < 186_715_800 {
        debug!("Using reward of 0.264865774 VE for daa_score {} < 186715800", daa_score);
        0.264865774
    } else if daa_score < 189_345_600 {
        debug!("Using reward of 0.257325559 VE for daa_score {} < 189345600", daa_score);
        0.257325559
    } else if daa_score < 191_975_400 {
        debug!("Using reward of 0.25 VE for daa_score {} < 191975400", daa_score);
        0.25
    } else if daa_score < 194_605_200 {
        debug!("Using reward of 0.242882985 VE for daa_score {} < 194605200", daa_score);
        0.242882985
    } else if daa_score < 197_235_000 {
        debug!("Using reward of 0.235968578 VE for daa_score {} < 197235000", daa_score);
        0.235968578
    } else if daa_score < 199_864_800 {
        debug!("Using reward of 0.229251011 VE for daa_score {} < 199864800", daa_score);
        0.229251011
    } else if daa_score < 202_494_600 {
        debug!("Using reward of 0.22272468 VE for daa_score {} < 202494600", daa_score);
        0.22272468
    } else if daa_score < 205_124_400 {
        debug!("Using reward of 0.21638414 VE for daa_score {} < 205124400", daa_score);
        0.21638414
    } else if daa_score < 207_754_200 {
        debug!("Using reward of 0.210224104 VE for daa_score {} < 207754200", daa_score);
        0.210224104
    } else if daa_score < 210_384_000 {
        debug!("Using reward of 0.204239432 VE for daa_score {} < 210384000", daa_score);
        0.204239432
    } else if daa_score < 213_013_800 {
        debug!("Using reward of 0.198425131 VE for daa_score {} < 213013800", daa_score);
        0.198425131
    } else if daa_score < 215_643_600 {
        debug!("Using reward of 0.192776353 VE for daa_score {} < 215643600", daa_score);
        0.192776353
    } else if daa_score < 218_273_400 {
        debug!("Using reward of 0.187288385 VE for daa_score {} < 218273400", daa_score);
        0.187288385
    } else if daa_score < 220_903_200 {
        debug!("Using reward of 0.181956648 VE for daa_score {} < 220903200", daa_score);
        0.181956648
    } else if daa_score < 223_533_000 {
        debug!("Using reward of 0.176776695 VE for daa_score {} < 223533000", daa_score);
        0.176776695
    } else if daa_score < 226_162_800 {
        debug!("Using reward of 0.171744206 VE for daa_score {} < 226162800", daa_score);
        0.171744206
    } else if daa_score < 228_792_600 {
        debug!("Using reward of 0.166854982 VE for daa_score {} < 228792600", daa_score);
        0.166854982
    } else if daa_score < 231_422_400 {
        debug!("Using reward of 0.162104944 VE for daa_score {} < 231422400", daa_score);
        0.162104944
    } else if daa_score < 234_052_200 {
        debug!("Using reward of 0.157490131 VE for daa_score {} < 234052200", daa_score);
        0.157490131
    } else if daa_score < 236_682_000 {
        debug!("Using reward of 0.153006693 VE for daa_score {} < 236682000", daa_score);
        0.153006693
    } else if daa_score < 239_311_800 {
        debug!("Using reward of 0.148650889 VE for daa_score {} < 239311800", daa_score);
        0.148650889
    } else if daa_score < 241_941_600 {
        debug!("Using reward of 0.144419087 VE for daa_score {} < 241941600", daa_score);
        0.144419087
    } else if daa_score < 244_571_400 {
        debug!("Using reward of 0.140307756 VE for daa_score {} < 244571400", daa_score);
        0.140307756
    } else if daa_score < 247_201_200 {
        debug!("Using reward of 0.136313467 VE for daa_score {} < 247201200", daa_score);
        0.136313467
    } else if daa_score < 249_831_000 {
        debug!("Using reward of 0.132432887 VE for daa_score {} < 249831000", daa_score);
        0.132432887
    } else if daa_score < 252_460_800 {
        debug!("Using reward of 0.12866278 VE for daa_score {} < 252460800", daa_score);
        0.12866278
    } else if daa_score < 255_090_600 {
        debug!("Using reward of 0.125 VE for daa_score {} < 255090600", daa_score);
        0.125
    } else if daa_score < 257_720_400 {
        debug!("Using reward of 0.121441493 VE for daa_score {} < 257720400", daa_score);
        0.121441493
    } else if daa_score < 260_350_200 {
        debug!("Using reward of 0.117984289 VE for daa_score {} < 260350200", daa_score);
        0.117984289
    } else if daa_score < 262_980_000 {
        debug!("Using reward of 0.114625505 VE for daa_score {} < 262980000", daa_score);
        0.114625505
    } else if daa_score < 265_609_800 {
        debug!("Using reward of 0.11136234 VE for daa_score {} < 265609800", daa_score);
        0.11136234
    } else if daa_score < 268_239_600 {
        debug!("Using reward of 0.10819207 VE for daa_score {} < 268239600", daa_score);
        0.10819207
    } else if daa_score < 270_869_400 {
        debug!("Using reward of 0.105112052 VE for daa_score {} < 270869400", daa_score);
        0.105112052
    } else if daa_score < 273_499_200 {
        debug!("Using reward of 0.102119716 VE for daa_score {} < 273499200", daa_score);
        0.102119716
    } else if daa_score < 276_129_000 {
        debug!("Using reward of 0.099212566 VE for daa_score {} < 276129000", daa_score);
        0.099212566
    } else if daa_score < 278_758_800 {
        debug!("Using reward of 0.096388177 VE for daa_score {} < 278758800", daa_score);
        0.096388177
    } else if daa_score < 281_388_600 {
        debug!("Using reward of 0.093644192 VE for daa_score {} < 281388600", daa_score);
        0.093644192
    } else if daa_score < 284_018_400 {
        debug!("Using reward of 0.090978324 VE for daa_score {} < 284018400", daa_score);
        0.090978324
    } else if daa_score < 286_648_200 {
        debug!("Using reward of 0.088388348 VE for daa_score {} < 286648200", daa_score);
        0.088388348
    } else if daa_score < 289_278_000 {
        debug!("Using reward of 0.085872103 VE for daa_score {} < 289278000", daa_score);
        0.085872103
    } else if daa_score < 291_907_800 {
        debug!("Using reward of 0.083427491 VE for daa_score {} < 291907800", daa_score);
        0.083427491
    } else if daa_score < 294_537_600 {
        debug!("Using reward of 0.081052472 VE for daa_score {} < 294537600", daa_score);
        0.081052472
    } else if daa_score < 297_167_400 {
        debug!("Using reward of 0.078745066 VE for daa_score {} < 297167400", daa_score);
        0.078745066
    } else if daa_score < 299_797_200 {
        debug!("Using reward of 0.076503346 VE for daa_score {} < 299797200", daa_score);
        0.076503346
    } else if daa_score < 302_427_000 {
        debug!("Using reward of 0.074325445 VE for daa_score {} < 302427000", daa_score);
        0.074325445
    } else if daa_score < 305_056_800 {
        debug!("Using reward of 0.072209544 VE for daa_score {} < 305056800", daa_score);
        0.072209544
    } else if daa_score < 307_686_600 {
        debug!("Using reward of 0.070153878 VE for daa_score {} < 307686600", daa_score);
        0.070153878
    } else if daa_score < 310_316_400 {
        debug!("Using reward of 0.068156733 VE for daa_score {} < 310316400", daa_score);
        0.068156733
    } else if daa_score < 312_946_200 {
        debug!("Using reward of 0.066216443 VE for daa_score {} < 312946200", daa_score);
        0.066216443
    } else if daa_score < 315_576_000 {
        debug!("Using reward of 0.06433139 VE for daa_score {} < 315576000", daa_score);
        0.06433139
    } else if daa_score < 318_205_800 {
        debug!("Using reward of 0.0625 VE for daa_score {} < 318205800", daa_score);
        0.0625
    } else if daa_score < 320_835_600 {
        debug!("Using reward of 0.060720746 VE for daa_score {} < 320835600", daa_score);
        0.060720746
    } else if daa_score < 323_465_400 {
        debug!("Using reward of 0.058992145 VE for daa_score {} < 323465400", daa_score);
        0.058992145
    } else if daa_score < 326_095_200 {
        debug!("Using reward of 0.057312753 VE for daa_score {} < 326095200", daa_score);
        0.057312753
    } else if daa_score < 328_725_000 {
        debug!("Using reward of 0.05568117 VE for daa_score {} < 328725000", daa_score);
        0.05568117
    } else if daa_score < 331_354_800 {
        debug!("Using reward of 0.054096035 VE for daa_score {} < 331354800", daa_score);
        0.054096035
    } else if daa_score < 333_984_600 {
        debug!("Using reward of 0.052556026 VE for daa_score {} < 333984600", daa_score);
        0.052556026
    } else if daa_score < 336_614_400 {
        debug!("Using reward of 0.051059858 VE for daa_score {} < 336614400", daa_score);
        0.051059858
    } else if daa_score < 339_244_200 {
        debug!("Using reward of 0.049606283 VE for daa_score {} < 339244200", daa_score);
        0.049606283
    } else if daa_score < 341_874_000 {
        debug!("Using reward of 0.048194088 VE for daa_score {} < 341874000", daa_score);
        0.048194088
    } else if daa_score < 344_503_800 {
        debug!("Using reward of 0.046822096 VE for daa_score {} < 344503800", daa_score);
        0.046822096
    } else if daa_score < 347_133_600 {
        debug!("Using reward of 0.045489162 VE for daa_score {} < 347133600", daa_score);
        0.045489162
    } else if daa_score < 349_763_400 {
        debug!("Using reward of 0.044194174 VE for daa_score {} < 349763400", daa_score);
        0.044194174
    } else if daa_score < 352_393_200 {
        debug!("Using reward of 0.042936051 VE for daa_score {} < 352393200", daa_score);
        0.042936051
    } else if daa_score < 355_023_000 {
        debug!("Using reward of 0.041713745 VE for daa_score {} < 355023000", daa_score);
        0.041713745
    } else if daa_score < 357_652_800 {
        debug!("Using reward of 0.040526236 VE for daa_score {} < 357652800", daa_score);
        0.040526236
    } else if daa_score < 360_282_600 {
        debug!("Using reward of 0.039372533 VE for daa_score {} < 360282600", daa_score);
        0.039372533
    } else if daa_score < 362_912_400 {
        debug!("Using reward of 0.038251673 VE for daa_score {} < 362912400", daa_score);
        0.038251673
    } else if daa_score < 365_542_200 {
        debug!("Using reward of 0.037162722 VE for daa_score {} < 365542200", daa_score);
        0.037162722
    } else if daa_score < 368_172_000 {
        debug!("Using reward of 0.036104772 VE for daa_score {} < 368172000", daa_score);
        0.036104772
    } else if daa_score < 370_801_800 {
        debug!("Using reward of 0.035076939 VE for daa_score {} < 370801800", daa_score);
        0.035076939
    } else if daa_score < 373_431_600 {
        debug!("Using reward of 0.034078367 VE for daa_score {} < 373431600", daa_score);
        0.034078367
    } else if daa_score < 376_061_400 {
        debug!("Using reward of 0.033108222 VE for daa_score {} < 376061400", daa_score);
        0.033108222
    } else if daa_score < 378_691_200 {
        debug!("Using reward of 0.032165695 VE for daa_score {} < 378691200", daa_score);
        0.032165695
    } else if daa_score < 381_321_000 {
        debug!("Using reward of 0.03125 VE for daa_score {} < 381321000", daa_score);
        0.03125
    } else if daa_score < 383_950_800 {
        debug!("Using reward of 0.030360373 VE for daa_score {} < 383950800", daa_score);
        0.030360373
    } else if daa_score < 386_580_600 {
        debug!("Using reward of 0.029496072 VE for daa_score {} < 386580600", daa_score);
        0.029496072
    } else if daa_score < 389_210_400 {
        debug!("Using reward of 0.028656376 VE for daa_score {} < 389210400", daa_score);
        0.028656376
    } else if daa_score < 391_840_200 {
        debug!("Using reward of 0.027840585 VE for daa_score {} < 391840200", daa_score);
        0.027840585
    } else if daa_score < 394_470_000 {
        debug!("Using reward of 0.027048018 VE for daa_score {} < 394470000", daa_score);
        0.027048018
    } else if daa_score < 397_099_800 {
        debug!("Using reward of 0.026278013 VE for daa_score {} < 397099800", daa_score);
        0.026278013
    } else if daa_score < 399_729_600 {
        debug!("Using reward of 0.025529929 VE for daa_score {} < 399729600", daa_score);
        0.025529929
    } else if daa_score < 402_359_400 {
        debug!("Using reward of 0.024803141 VE for daa_score {} < 402359400", daa_score);
        0.024803141
    } else if daa_score < 404_989_200 {
        debug!("Using reward of 0.024097044 VE for daa_score {} < 404989200", daa_score);
        0.024097044
    } else if daa_score < 407_619_000 {
        debug!("Using reward of 0.023411048 VE for daa_score {} < 407619000", daa_score);
        0.023411048
    } else if daa_score < 410_248_800 {
        debug!("Using reward of 0.022744581 VE for daa_score {} < 410248800", daa_score);
        0.022744581
    } else if daa_score < 412_878_600 {
        debug!("Using reward of 0.022097087 VE for daa_score {} < 412878600", daa_score);
        0.022097087
    } else if daa_score < 415_508_400 {
        debug!("Using reward of 0.021468026 VE for daa_score {} < 415508400", daa_score);
        0.021468026
    } else if daa_score < 418_138_200 {
        debug!("Using reward of 0.020856873 VE for daa_score {} < 418138200", daa_score);
        0.020856873
    } else if daa_score < 420_768_000 {
        debug!("Using reward of 0.020263118 VE for daa_score {} < 420768000", daa_score);
        0.020263118
    } else if daa_score < 423_397_800 {
        debug!("Using reward of 0.019686266 VE for daa_score {} < 423397800", daa_score);
        0.019686266
    } else if daa_score < 426_027_600 {
        debug!("Using reward of 0.019125837 VE for daa_score {} < 426027600", daa_score);
        0.019125837
    } else if daa_score < 428_657_400 {
        debug!("Using reward of 0.018581361 VE for daa_score {} < 428657400", daa_score);
        0.018581361
    } else if daa_score < 431_287_200 {
        debug!("Using reward of 0.018052386 VE for daa_score {} < 431287200", daa_score);
        0.018052386
    } else if daa_score < 433_917_000 {
        debug!("Using reward of 0.01753847 VE for daa_score {} < 433917000", daa_score);
        0.01753847
    } else if daa_score < 436_546_800 {
        debug!("Using reward of 0.017039183 VE for daa_score {} < 436546800", daa_score);
        0.017039183
    } else if daa_score < 439_176_600 {
        debug!("Using reward of 0.016554111 VE for daa_score {} < 439176600", daa_score);
        0.016554111
    } else if daa_score < 441_806_400 {
        debug!("Using reward of 0.016082847 VE for daa_score {} < 441806400", daa_score);
        0.016082847
    } else if daa_score < 444_436_200 {
        debug!("Using reward of 0.015625 VE for daa_score {} < 444436200", daa_score);
        0.015625
    } else if daa_score < 447_066_000 {
        debug!("Using reward of 0.015180187 VE for daa_score {} < 447066000", daa_score);
        0.015180187
    } else if daa_score < 449_695_800 {
        debug!("Using reward of 0.014748036 VE for daa_score {} < 449695800", daa_score);
        0.014748036
    } else if daa_score < 452_325_600 {
        debug!("Using reward of 0.014328188 VE for daa_score {} < 452325600", daa_score);
        0.014328188
    } else if daa_score < 454_955_400 {
        debug!("Using reward of 0.013920292 VE for daa_score {} < 454955400", daa_score);
        0.013920292
    } else if daa_score < 457_585_200 {
        debug!("Using reward of 0.013524009 VE for daa_score {} < 457585200", daa_score);
        0.013524009
    } else if daa_score < 460_215_000 {
        debug!("Using reward of 0.013139006 VE for daa_score {} < 460215000", daa_score);
        0.013139006
    } else if daa_score < 462_844_800 {
        debug!("Using reward of 0.012764964 VE for daa_score {} < 462844800", daa_score);
        0.012764964
    } else if daa_score < 465_474_600 {
        debug!("Using reward of 0.012401571 VE for daa_score {} < 465474600", daa_score);
        0.012401571
    } else if daa_score < 468_104_400 {
        debug!("Using reward of 0.012048522 VE for daa_score {} < 468104400", daa_score);
        0.012048522
    } else if daa_score < 470_734_200 {
        debug!("Using reward of 0.011705524 VE for daa_score {} < 470734200", daa_score);
        0.011705524
    } else if daa_score < 473_364_000 {
        debug!("Using reward of 0.01137229 VE for daa_score {} < 473364000", daa_score);
        0.01137229
    } else if daa_score < 475_993_800 {
        debug!("Using reward of 0.011048543 VE for daa_score {} < 475993800", daa_score);
        0.011048543
    } else if daa_score < 478_623_600 {
        debug!("Using reward of 0.010734013 VE for daa_score {} < 478623600", daa_score);
        0.010734013
    } else if daa_score < 481_253_400 {
        debug!("Using reward of 0.010428436 VE for daa_score {} < 481253400", daa_score);
        0.010428436
    } else if daa_score < 483_883_200 {
        debug!("Using reward of 0.010131559 VE for daa_score {} < 483883200", daa_score);
        0.010131559
    } else if daa_score < 486_513_000 {
        debug!("Using reward of 0.009843133 VE for daa_score {} < 486513000", daa_score);
        0.009843133
    } else if daa_score < 489_142_800 {
        debug!("Using reward of 0.009562918 VE for daa_score {} < 489142800", daa_score);
        0.009562918
    } else if daa_score < 491_772_600 {
        debug!("Using reward of 0.009290681 VE for daa_score {} < 491772600", daa_score);
        0.009290681
    } else if daa_score < 494_402_400 {
        debug!("Using reward of 0.009026193 VE for daa_score {} < 494402400", daa_score);
        0.009026193
    } else if daa_score < 497_032_200 {
        debug!("Using reward of 0.008769235 VE for daa_score {} < 497032200", daa_score);
        0.008769235
    } else if daa_score < 499_662_000 {
        debug!("Using reward of 0.008519592 VE for daa_score {} < 499662000", daa_score);
        0.008519592
    } else if daa_score < 502_291_800 {
        debug!("Using reward of 0.008277055 VE for daa_score {} < 502291800", daa_score);
        0.008277055
    } else if daa_score < 504_921_600 {
        debug!("Using reward of 0.008041424 VE for daa_score {} < 504921600", daa_score);
        0.008041424
    } else if daa_score < 507_551_400 {
        debug!("Using reward of 0.0078125 VE for daa_score {} < 507551400", daa_score);
        0.0078125
    } else if daa_score < 510_181_200 {
        debug!("Using reward of 0.007590093 VE for daa_score {} < 510181200", daa_score);
        0.007590093
    } else if daa_score < 512_811_000 {
        debug!("Using reward of 0.007374018 VE for daa_score {} < 512811000", daa_score);
        0.007374018
    } else if daa_score < 515_440_800 {
        debug!("Using reward of 0.007164094 VE for daa_score {} < 515440800", daa_score);
        0.007164094
    } else if daa_score < 518_070_600 {
        debug!("Using reward of 0.006960146 VE for daa_score {} < 518070600", daa_score);
        0.006960146
    } else if daa_score < 520_700_400 {
        debug!("Using reward of 0.006762004 VE for daa_score {} < 520700400", daa_score);
        0.006762004
    } else if daa_score < 523_330_200 {
        debug!("Using reward of 0.006569503 VE for daa_score {} < 523330200", daa_score);
        0.006569503
    } else if daa_score < 525_960_000 {
        debug!("Using reward of 0.006382482 VE for daa_score {} < 525960000", daa_score);
        0.006382482
    } else if daa_score < 528_589_800 {
        debug!("Using reward of 0.006200785 VE for daa_score {} < 528589800", daa_score);
        0.006200785
    } else if daa_score < 531_219_600 {
        debug!("Using reward of 0.006024261 VE for daa_score {} < 531219600", daa_score);
        0.006024261
    } else if daa_score < 533_849_400 {
        debug!("Using reward of 0.005852762 VE for daa_score {} < 533849400", daa_score);
        0.005852762
    } else if daa_score < 536_479_200 {
        debug!("Using reward of 0.005686145 VE for daa_score {} < 536479200", daa_score);
        0.005686145
    } else if daa_score < 539_109_000 {
        debug!("Using reward of 0.005524272 VE for daa_score {} < 539109000", daa_score);
        0.005524272
    } else if daa_score < 541_738_800 {
        debug!("Using reward of 0.005367006 VE for daa_score {} < 541738800", daa_score);
        0.005367006
    } else if daa_score < 544_368_600 {
        debug!("Using reward of 0.005214218 VE for daa_score {} < 544368600", daa_score);
        0.005214218
    } else if daa_score < 546_998_400 {
        debug!("Using reward of 0.00506578 VE for daa_score {} < 546998400", daa_score);
        0.00506578
    } else if daa_score < 549_628_200 {
        debug!("Using reward of 0.004921567 VE for daa_score {} < 549628200", daa_score);
        0.004921567
    } else if daa_score < 552_258_000 {
        debug!("Using reward of 0.004781459 VE for daa_score {} < 552258000", daa_score);
        0.004781459
    } else if daa_score < 554_887_800 {
        debug!("Using reward of 0.00464534 VE for daa_score {} < 554887800", daa_score);
        0.00464534
    } else if daa_score < 557_517_600 {
        debug!("Using reward of 0.004513096 VE for daa_score {} < 557517600", daa_score);
        0.004513096
    } else if daa_score < 560_147_400 {
        debug!("Using reward of 0.004384617 VE for daa_score {} < 560147400", daa_score);
        0.004384617
    } else if daa_score < 562_777_200 {
        debug!("Using reward of 0.004259796 VE for daa_score {} < 562777200", daa_score);
        0.004259796
    } else if daa_score < 565_407_000 {
        debug!("Using reward of 0.004138528 VE for daa_score {} < 565407000", daa_score);
        0.004138528
    } else if daa_score < 568_036_800 {
        debug!("Using reward of 0.004020712 VE for daa_score {} < 568036800", daa_score);
        0.004020712
    } else if daa_score < 570_666_600 {
        debug!("Using reward of 0.00390625 VE for daa_score {} < 570666600", daa_score);
        0.00390625
    } else if daa_score < 573_296_400 {
        debug!("Using reward of 0.003795047 VE for daa_score {} < 573296400", daa_score);
        0.003795047
    } else if daa_score < 575_926_200 {
        debug!("Using reward of 0.003687009 VE for daa_score {} < 575926200", daa_score);
        0.003687009
    } else if daa_score < 578_556_000 {
        debug!("Using reward of 0.003582047 VE for daa_score {} < 578556000", daa_score);
        0.003582047
    } else if daa_score < 581_185_800 {
        debug!("Using reward of 0.003480073 VE for daa_score {} < 581185800", daa_score);
        0.003480073
    } else if daa_score < 583_815_600 {
        debug!("Using reward of 0.003381002 VE for daa_score {} < 583815600", daa_score);
        0.003381002
    } else if daa_score < 586_445_400 {
        debug!("Using reward of 0.003284752 VE for daa_score {} < 586445400", daa_score);
        0.003284752
    } else if daa_score < 589_075_200 {
        debug!("Using reward of 0.003191241 VE for daa_score {} < 589075200", daa_score);
        0.003191241
    } else if daa_score < 591_705_000 {
        debug!("Using reward of 0.003100393 VE for daa_score {} < 591705000", daa_score);
        0.003100393
    } else if daa_score < 594_334_800 {
        debug!("Using reward of 0.003012131 VE for daa_score {} < 594334800", daa_score);
        0.003012131
    } else if daa_score < 596_964_600 {
        debug!("Using reward of 0.002926381 VE for daa_score {} < 596964600", daa_score);
        0.002926381
    } else if daa_score < 599_594_400 {
        debug!("Using reward of 0.002843073 VE for daa_score {} < 599594400", daa_score);
        0.002843073
    } else if daa_score < 602_224_200 {
        debug!("Using reward of 0.002762136 VE for daa_score {} < 602224200", daa_score);
        0.002762136
    } else if daa_score < 604_854_000 {
        debug!("Using reward of 0.002683503 VE for daa_score {} < 604854000", daa_score);
        0.002683503
    } else if daa_score < 607_483_800 {
        debug!("Using reward of 0.002607109 VE for daa_score {} < 607483800", daa_score);
        0.002607109
    } else if daa_score < 610_113_600 {
        debug!("Using reward of 0.00253289 VE for daa_score {} < 610113600", daa_score);
        0.00253289
    } else if daa_score < 612_743_400 {
        debug!("Using reward of 0.002460783 VE for daa_score {} < 612743400", daa_score);
        0.002460783
    } else if daa_score < 615_373_200 {
        debug!("Using reward of 0.00239073 VE for daa_score {} < 615373200", daa_score);
        0.00239073
    } else if daa_score < 618_003_000 {
        debug!("Using reward of 0.00232267 VE for daa_score {} < 618003000", daa_score);
        0.00232267
    } else if daa_score < 620_632_800 {
        debug!("Using reward of 0.002256548 VE for daa_score {} < 620632800", daa_score);
        0.002256548
    } else if daa_score < 623_262_600 {
        debug!("Using reward of 0.002192309 VE for daa_score {} < 623262600", daa_score);
        0.002192309
    } else if daa_score < 625_892_400 {
        debug!("Using reward of 0.002129898 VE for daa_score {} < 625892400", daa_score);
        0.002129898
    } else if daa_score < 628_522_200 {
        debug!("Using reward of 0.002069264 VE for daa_score {} < 628522200", daa_score);
        0.002069264
    } else if daa_score < 631_152_000 {
        debug!("Using reward of 0.002010356 VE for daa_score {} < 631152000", daa_score);
        0.002010356
    } else if daa_score < 633_781_800 {
        debug!("Using reward of 0.001953125 VE for daa_score {} < 633781800", daa_score);
        0.001953125
    } else if daa_score < 636_411_600 {
        debug!("Using reward of 0.001897523 VE for daa_score {} < 636411600", daa_score);
        0.001897523
    } else if daa_score < 639_041_400 {
        debug!("Using reward of 0.001843505 VE for daa_score {} < 639041400", daa_score);
        0.001843505
    } else if daa_score < 641_671_200 {
        debug!("Using reward of 0.001791024 VE for daa_score {} < 641671200", daa_score);
        0.001791024
    } else if daa_score < 644_301_000 {
        debug!("Using reward of 0.001740037 VE for daa_score {} < 644301000", daa_score);
        0.001740037
    } else if daa_score < 646_930_800 {
        debug!("Using reward of 0.001690501 VE for daa_score {} < 646930800", daa_score);
        0.001690501
    } else if daa_score < 649_560_600 {
        debug!("Using reward of 0.001642376 VE for daa_score {} < 649560600", daa_score);
        0.001642376
    } else if daa_score < 652_190_400 {
        debug!("Using reward of 0.001595621 VE for daa_score {} < 652190400", daa_score);
        0.001595621
    } else if daa_score < 654_820_200 {
        debug!("Using reward of 0.001550196 VE for daa_score {} < 654820200", daa_score);
        0.001550196
    } else if daa_score < 657_450_000 {
        debug!("Using reward of 0.001506065 VE for daa_score {} < 657450000", daa_score);
        0.001506065
    } else if daa_score < 660_079_800 {
        debug!("Using reward of 0.001463191 VE for daa_score {} < 660079800", daa_score);
        0.001463191
    } else if daa_score < 662_709_600 {
        debug!("Using reward of 0.001421536 VE for daa_score {} < 662709600", daa_score);
        0.001421536
    } else if daa_score < 665_339_400 {
        debug!("Using reward of 0.001381068 VE for daa_score {} < 665339400", daa_score);
        0.001381068
    } else if daa_score < 667_969_200 {
        debug!("Using reward of 0.001341752 VE for daa_score {} < 667969200", daa_score);
        0.001341752
    } else if daa_score < 670_599_000 {
        debug!("Using reward of 0.001303555 VE for daa_score {} < 670599000", daa_score);
        0.001303555
    } else if daa_score < 673_228_800 {
        debug!("Using reward of 0.001266445 VE for daa_score {} < 673228800", daa_score);
        0.001266445
    } else if daa_score < 675_858_600 {
        debug!("Using reward of 0.001230392 VE for daa_score {} < 675858600", daa_score);
        0.001230392
    } else if daa_score < 678_488_400 {
        debug!("Using reward of 0.001195365 VE for daa_score {} < 678488400", daa_score);
        0.001195365
    } else if daa_score < 681_118_200 {
        debug!("Using reward of 0.001161335 VE for daa_score {} < 681118200", daa_score);
        0.001161335
    } else if daa_score < 683_748_000 {
        debug!("Using reward of 0.001128274 VE for daa_score {} < 683748000", daa_score);
        0.001128274
    } else if daa_score < 686_377_800 {
        debug!("Using reward of 0.001096154 VE for daa_score {} < 686377800", daa_score);
        0.001096154
    } else if daa_score < 689_007_600 {
        debug!("Using reward of 0.001064949 VE for daa_score {} < 689007600", daa_score);
        0.001064949
    } else if daa_score < 691_637_400 {
        debug!("Using reward of 0.001034632 VE for daa_score {} < 691637400", daa_score);
        0.001034632
    } else if daa_score < 694_267_200 {
        debug!("Using reward of 0.001005178 VE for daa_score {} < 694267200", daa_score);
        0.001005178
    } else if daa_score < 696_897_000 {
        debug!("Using reward of 0.000976563 VE for daa_score {} < 696897000", daa_score);
        0.000976563
    } else if daa_score < 699_526_800 {
        debug!("Using reward of 0.000948762 VE for daa_score {} < 699526800", daa_score);
        0.000948762
    } else if daa_score < 702_156_600 {
        debug!("Using reward of 0.000921752 VE for daa_score {} < 702156600", daa_score);
        0.000921752
    } else if daa_score < 704_786_400 {
        debug!("Using reward of 0.000895512 VE for daa_score {} < 704786400", daa_score);
        0.000895512
    } else if daa_score < 707_416_200 {
        debug!("Using reward of 0.000870018 VE for daa_score {} < 707416200", daa_score);
        0.000870018
    } else if daa_score < 710_046_000 {
        debug!("Using reward of 0.000845251 VE for daa_score {} < 710046000", daa_score);
        0.000845251
    } else if daa_score < 712_675_800 {
        debug!("Using reward of 0.000821188 VE for daa_score {} < 712675800", daa_score);
        0.000821188
    } else if daa_score < 715_305_600 {
        debug!("Using reward of 0.00079781 VE for daa_score {} < 715305600", daa_score);
        0.00079781
    } else if daa_score < 717_935_400 {
        debug!("Using reward of 0.000775098 VE for daa_score {} < 717935400", daa_score);
        0.000775098
    } else if daa_score < 720_565_200 {
        debug!("Using reward of 0.000753033 VE for daa_score {} < 720565200", daa_score);
        0.000753033
    } else if daa_score < 723_195_000 {
        debug!("Using reward of 0.000731595 VE for daa_score {} < 723195000", daa_score);
        0.000731595
    } else if daa_score < 725_824_800 {
        debug!("Using reward of 0.000710768 VE for daa_score {} < 725824800", daa_score);
        0.000710768
    } else if daa_score < 728_454_600 {
        debug!("Using reward of 0.000690534 VE for daa_score {} < 728454600", daa_score);
        0.000690534
    } else if daa_score < 731_084_400 {
        debug!("Using reward of 0.000670876 VE for daa_score {} < 731084400", daa_score);
        0.000670876
    } else if daa_score < 733_714_200 {
        debug!("Using reward of 0.000651777 VE for daa_score {} < 733714200", daa_score);
        0.000651777
    } else if daa_score < 736_344_000 {
        debug!("Using reward of 0.000633222 VE for daa_score {} < 736344000", daa_score);
        0.000633222
    } else if daa_score < 738_973_800 {
        debug!("Using reward of 0.000615196 VE for daa_score {} < 738973800", daa_score);
        0.000615196
    } else if daa_score < 741_603_600 {
        debug!("Using reward of 0.000597682 VE for daa_score {} < 741603600", daa_score);
        0.000597682
    } else if daa_score < 744_233_400 {
        debug!("Using reward of 0.000580668 VE for daa_score {} < 744233400", daa_score);
        0.000580668
    } else if daa_score < 746_863_200 {
        debug!("Using reward of 0.000564137 VE for daa_score {} < 746863200", daa_score);
        0.000564137
    } else if daa_score < 749_493_000 {
        debug!("Using reward of 0.000548077 VE for daa_score {} < 749493000", daa_score);
        0.000548077
    } else if daa_score < 752_122_800 {
        debug!("Using reward of 0.000532474 VE for daa_score {} < 752122800", daa_score);
        0.000532474
    } else if daa_score < 754_752_600 {
        debug!("Using reward of 0.000517316 VE for daa_score {} < 754752600", daa_score);
        0.000517316
    } else if daa_score < 757_382_400 {
        debug!("Using reward of 0.000502589 VE for daa_score {} < 757382400", daa_score);
        0.000502589
    } else if daa_score < 760_012_200 {
        debug!("Using reward of 0.000488281 VE for daa_score {} < 760012200", daa_score);
        0.000488281
    } else if daa_score < 762_642_000 {
        debug!("Using reward of 0.000474381 VE for daa_score {} < 762642000", daa_score);
        0.000474381
    } else if daa_score < 765_271_800 {
        debug!("Using reward of 0.000460876 VE for daa_score {} < 765271800", daa_score);
        0.000460876
    } else if daa_score < 767_901_600 {
        debug!("Using reward of 0.000447756 VE for daa_score {} < 767901600", daa_score);
        0.000447756
    } else if daa_score < 770_531_400 {
        debug!("Using reward of 0.000435009 VE for daa_score {} < 770531400", daa_score);
        0.000435009
    } else if daa_score < 773_161_200 {
        debug!("Using reward of 0.000422625 VE for daa_score {} < 773161200", daa_score);
        0.000422625
    } else if daa_score < 775_791_000 {
        debug!("Using reward of 0.000410594 VE for daa_score {} < 775791000", daa_score);
        0.000410594
    } else if daa_score < 778_420_800 {
        debug!("Using reward of 0.000398905 VE for daa_score {} < 778420800", daa_score);
        0.000398905
    } else if daa_score < 781_050_600 {
        debug!("Using reward of 0.000387549 VE for daa_score {} < 781050600", daa_score);
        0.000387549
    } else if daa_score < 783_680_400 {
        debug!("Using reward of 0.000376516 VE for daa_score {} < 783680400", daa_score);
        0.000376516
    } else if daa_score < 786_310_200 {
        debug!("Using reward of 0.000365798 VE for daa_score {} < 786310200", daa_score);
        0.000365798
    } else if daa_score < 788_940_000 {
        debug!("Using reward of 0.000355384 VE for daa_score {} < 788940000", daa_score);
        0.000355384
    } else if daa_score < 791_569_800 {
        debug!("Using reward of 0.000345267 VE for daa_score {} < 791569800", daa_score);
        0.000345267
    } else if daa_score < 794_199_600 {
        debug!("Using reward of 0.000335438 VE for daa_score {} < 794199600", daa_score);
        0.000335438
    } else if daa_score < 796_829_400 {
        debug!("Using reward of 0.000325889 VE for daa_score {} < 796829400", daa_score);
        0.000325889
    } else if daa_score < 799_459_200 {
        debug!("Using reward of 0.000316611 VE for daa_score {} < 799459200", daa_score);
        0.000316611
    } else if daa_score < 802_089_000 {
        debug!("Using reward of 0.000307598 VE for daa_score {} < 802089000", daa_score);
        0.000307598
    } else if daa_score < 804_718_800 {
        debug!("Using reward of 0.000298841 VE for daa_score {} < 804718800", daa_score);
        0.000298841
    } else if daa_score < 807_348_600 {
        debug!("Using reward of 0.000290334 VE for daa_score {} < 807348600", daa_score);
        0.000290334
    } else if daa_score < 809_978_400 {
        debug!("Using reward of 0.000282069 VE for daa_score {} < 809978400", daa_score);
        0.000282069
    } else if daa_score < 812_608_200 {
        debug!("Using reward of 0.000274039 VE for daa_score {} < 812608200", daa_score);
        0.000274039
    } else if daa_score < 815_238_000 {
        debug!("Using reward of 0.000266237 VE for daa_score {} < 815238000", daa_score);
        0.000266237
    } else if daa_score < 817_867_800 {
        debug!("Using reward of 0.000258658 VE for daa_score {} < 817867800", daa_score);
        0.000258658
    } else if daa_score < 820_497_600 {
        debug!("Using reward of 0.000251294 VE for daa_score {} < 820497600", daa_score);
        0.000251294
    } else if daa_score < 823_127_400 {
        debug!("Using reward of 0.000244141 VE for daa_score {} < 823127400", daa_score);
        0.000244141
    } else if daa_score < 825_757_200 {
        debug!("Using reward of 0.00023719 VE for daa_score {} < 825757200", daa_score);
        0.00023719
    } else if daa_score < 828_387_000 {
        debug!("Using reward of 0.000230438 VE for daa_score {} < 828387000", daa_score);
        0.000230438
    } else if daa_score < 833_646_600 {
        debug!("Using reward of 0.000223878 VE for daa_score {} < 833646600", daa_score);
        0.000223878
    } else if daa_score < 836_276_400 {
        debug!("Using reward of 0.000211313 VE for daa_score {} < 836276400", daa_score);
        0.000211313
    } else if daa_score < 838_906_200 {
        debug!("Using reward of 0.000205297 VE for daa_score {} < 838906200", daa_score);
        0.000205297
    } else if daa_score < 841_536_000 {
        debug!("Using reward of 0.000199453 VE for daa_score {} < 841536000", daa_score);
        0.000199453
    } else if daa_score < 844_165_800 {
        debug!("Using reward of 0.000193775 VE for daa_score {} < 844165800", daa_score);
        0.000193775
    } else if daa_score < 846_795_600 {
        debug!("Using reward of 0.000188258 VE for daa_score {} < 846795600", daa_score);
        0.000188258
    } else if daa_score < 849_425_400 {
        debug!("Using reward of 0.000182899 VE for daa_score {} < 849425400", daa_score);
        0.000182899
    } else if daa_score < 852_055_200 {
        debug!("Using reward of 0.000177692 VE for daa_score {} < 852055200", daa_score);
        0.000177692
    } else if daa_score < 854_685_000 {
        debug!("Using reward of 0.000172633 VE for daa_score {} < 854685000", daa_score);
        0.000172633
    } else if daa_score < 857_314_800 {
        debug!("Using reward of 0.000167719 VE for daa_score {} < 857314800", daa_score);
        0.000167719
    } else if daa_score < 859_944_600 {
        debug!("Using reward of 0.000162944 VE for daa_score {} < 859944600", daa_score);
        0.000162944
    } else if daa_score < 862_574_400 {
        debug!("Using reward of 0.000158306 VE for daa_score {} < 862574400", daa_score);
        0.000158306
    } else if daa_score < 865_204_200 {
        debug!("Using reward of 0.000153799 VE for daa_score {} < 865204200", daa_score);
        0.000153799
    } else if daa_score < 867_834_000 {
        debug!("Using reward of 0.000149421 VE for daa_score {} < 867834000", daa_score);
        0.000149421
    } else if daa_score < 870_463_800 {
        debug!("Using reward of 0.000145167 VE for daa_score {} < 870463800", daa_score);
        0.000145167
    } else if daa_score < 873_093_600 {
        debug!("Using reward of 0.000141034 VE for daa_score {} < 873093600", daa_score);
        0.000141034
    } else if daa_score < 875_723_400 {
        debug!("Using reward of 0.000137019 VE for daa_score {} < 875723400", daa_score);
        0.000137019
    } else if daa_score < 878_353_200 {
        debug!("Using reward of 0.000133119 VE for daa_score {} < 878353200", daa_score);
        0.000133119
    } else if daa_score < 880_983_000 {
        debug!("Using reward of 0.000129329 VE for daa_score {} < 880983000", daa_score);
        0.000129329
    } else if daa_score < 883_612_800 {
        debug!("Using reward of 0.000125647 VE for daa_score {} < 883612800", daa_score);
        0.000125647
    } else if daa_score < 886_242_600 {
        debug!("Using reward of 0.00012207 VE for daa_score {} < 886242600", daa_score);
        0.00012207
    } else if daa_score < 888_872_400 {
        debug!("Using reward of 0.000118595 VE for daa_score {} < 888872400", daa_score);
        0.000118595
    } else if daa_score < 891_502_200 {
        debug!("Using reward of 0.000111939 VE for daa_score {} < 891502200", daa_score);
        0.000111939
    } else if daa_score < 894_132_000 {
        debug!("Using reward of 0.000115219 VE for daa_score {} < 894132000", daa_score);
        0.000115219
    } else if daa_score < 896_761_800 {
        debug!("Using reward of 0.000108752 VE for daa_score {} < 896761800", daa_score);
        0.000108752
    } else if daa_score < 899_391_600 {
        debug!("Using reward of 0.000105656 VE for daa_score {} < 899391600", daa_score);
        0.000105656
    } else if daa_score < 902_021_400 {
        debug!("Using reward of 0.000102648 VE for daa_score {} < 902021400", daa_score);
        0.000102648
    } else {
        debug!("Using reward of 0.0001 VE for daa_score {} >= 904651200", daa_score);
        0.0001
    };

    let reward_sompi = (reward_ve * 100_000_000.0) as u64;
    debug!("Selected reward: {} VE ({} sompi) for daa_score {}", reward_ve, reward_sompi, daa_score);
    Ok(reward_sompi)
}