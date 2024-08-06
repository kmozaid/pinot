/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.SampleQueries;


public class MultistageEngineQuickStart extends Quickstart {
  private static final String[] MULTI_STAGE_TABLE_DIRECTORIES = new String[]{
      "examples/batch/airlineStats",
      "examples/batch/baseballStats",
      "examples/batch/billing",
      "examples/batch/dimBaseballTeams",
      "examples/batch/githubEvents",
      "examples/batch/githubComplexTypeEvents",
      "examples/batch/ssb/customer",
      "examples/batch/ssb/dates",
      "examples/batch/ssb/lineorder",
      "examples/batch/ssb/part",
      "examples/batch/ssb/supplier",
      "examples/batch/starbucksStores",
      "examples/batch/fineFoodReviews",
  };

  @Override
  public List<String> types() {
    return Collections.singletonList("MULTI_STAGE");
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {

    printStatus(Quickstart.Color.YELLOW, "***** Multi-stage engine quickstart setup complete *****");
    Map<String, String> queryOptions = Collections.singletonMap("queryOptions",
        CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");
    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    printStatus(Quickstart.Color.CYAN, "Query : " + SampleQueries.COUNT_BASEBALL_STATS);
    printStatus(Quickstart.Color.YELLOW,
        prettyPrintResponse(runner.runQuery(SampleQueries.COUNT_BASEBALL_STATS, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.YELLOW, "Correlate the same player(s) with more than 160-run some year(s) and"
        + " with less than 2-run some other year(s)");
    printStatus(Quickstart.Color.CYAN, "Query : " + SampleQueries.BASEBALL_STATS_SELF_JOIN);
    printStatus(Quickstart.Color.YELLOW,
        prettyPrintResponse(runner.runQuery(SampleQueries.BASEBALL_STATS_SELF_JOIN, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.YELLOW, "Baseball Stats with joined team names");
    printStatus(Quickstart.Color.CYAN, "Query : " + SampleQueries.BASEBALL_JOIN_DIM_BASEBALL_TEAMS);
    printStatus(Quickstart.Color.YELLOW,
        prettyPrintResponse(runner.runQuery(SampleQueries.BASEBALL_JOIN_DIM_BASEBALL_TEAMS, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q4 =
        "select ProductId, UserId, l2_distance(embedding, ARRAY[-0.0013143676,-0.011042999,-0.015155246,-0"
            + ".0085545285,-0.0030792344,0.0115776565,0.000080961916,-0.042006962,-0.019419309,-0.016145352,0"
            + ".028119052,0.002938969,-0.025373155,-0.028990347,-0.025663586,0.030178476,0.024805494,-0.0019752644,0"
            + ".0016897834,-0.012541361,0.004617201,0.0038911225,0.023234522,0.006405995,-0.00057591253,-0.009439025,"
            + "0.010257513,-0.01675262,-0.0031254394,0.006254179,0.012911001,-0.018759236,-0.026785707,-0.0065413103,"
            + "-0.0036237934,0.0032541533,-0.014521576,-0.012382944,0.02459427,-0.034429338,0.021478731,-0"
            + ".00054950966,0.008224493,-0.015168447,-0.02154474,0.0039043238,0.0062310765,-0.006435699,0.0024703182,"
            + "-0.003976932,0.043881565,0.013320246,-0.00006471796,-0.00064687023,0.016607404,0.011128808,-0"
            + ".011452244,0.0031023368,0.023274127,-0.028594304,0.017531503,0.016924238,-0.04385516,0.0040297373,-0"
            + ".01611895,-0.011042999,-0.022231214,-0.004438982,0.02155794,-0.008811956,0.019749343,0.042218182,-0"
            + ".015062835,0.00083870353,0.028435888,-0.021848371,-0.013082621,0.007049565,0.02191438,-0.011089204,0"
            + ".010488539,-0.027036535,-0.011016596,0.033214808,0.014547979,0.014165138,-0.006151868,0.042059764,-0"
            + ".010825175,-0.015062835,0.024858298,0.0071815797,0.01225093,0.013142027,-0.033029985,0.020132186,0"
            + ".007947262,0.035063006,-0.0016790573,-0.02975603,-0.0047657173,-0.0129968105,0.0059901504,-0.01685823,"
            + "-0.025346752,-0.006844943,0.019049669,0.022350026,0.019551322,0.0054389904,-0.0070627667,0.017439093,"
            + "-0.0010602401,-0.02377578,-0.013861504,-0.010534744,0.03363725,-0.038468976,-0.01860082,-0.0012772387,"
            + "0.0030049763,0.020871466,0.020198192,-0.026627291,0.0066370205,0.0041122464,-0.03049531,-0.0068779467,"
            + "0.027379772,-0.010310319,0.03324121,0.023683371,-0.002339954,0.007425806,-0.0060099526,0.016039742,-0"
            + ".0359343,0.008178288,0.010026488,-0.009505032,0.012323538,0.016620604,-0.00866014,-0.006320186,-0"
            + ".017927546,0.018798841,-0.00635649,0.039498687,0.0021138794,0.0074390075,0.013901109,-0.02165035,-0"
            + ".0043069674,0.002587481,0.028356679,0.021584343,0.005386185,0.021742761,-0.02238963,-0.0042013563,0"
            + ".015155246,-0.002570979,-0.018019956,-0.0063036843,0.0130496165,0.014415965,0.043327104,-0.016818626,"
            + "-0.00021452329,-0.013716289,-0.006405995,0.019472115,-0.035775885,0.00948523,0.0050528487,-0"
            + ".005396086,0.0075380183,0.02228402,-0.03350524,-0.016514992,-0.009934079,0.011036398,0.023551356,0"
            + ".02312891,-0.009802064,-0.03794092,0.003703002,-0.005703019,0.0038812214,-0.009973682,-0.005950546,0"
            + ".024845097,0.0014414315,-0.008250896,-0.6353586,-0.03556466,0.001391101,-0.03860099,0.03765049,0"
            + ".029597614,0.009868071,-0.005287174,0.0044158795,-0.003739306,-0.015762512,0.029518405,-0.009775661,-0"
            + ".0146535905,0.031049771,-0.030257685,0.04578257,-0.0023284028,-0.007075968,0.02044902,-0.008184888,-0"
            + ".0023713075,-0.010495139,-0.021373121,0.028251067,0.004072642,0.027643802,-0.012270732,-0.0019010063,0"
            + ".009604042,-0.021518337,0.015326864,0.029254375,0.016184958,0.052330483,-0.0061188643,-0.009148593,-0"
            + ".0054950966,0.017795531,0.030996965,-0.041426096,-0.010574348,0.027274162,-0.016620604,-0.012957207,0"
            + ".010165104,0.028171858,0.0068515437,-0.0033383125,-0.008429115,0.02183517,0.006465402,-0.040317178,-0"
            + ".013663483,0.019274093,-0.005353181,0.044462427,-0.014033124,-0.0046403036,0.0004826774,-0.006399395,0"
            + ".011590858,-0.018310389,-0.030970562,-0.008448917,0.011769078,-0.019089272,-0.004033038,0.02275927,-0"
            + ".010019888,-0.000750419,0.026521679,-0.010151902,-0.009359816,0.028039845,0.01668661,0.012198124,0"
            + ".016066143,0.008125482,0.002726096,0.023340134,-0.022165205,0.020884667,-0.039551493,0.03527423,0"
            + ".009326813,-0.013953915,0.008336705,-0.013808699,-0.028805528,0.0017788932,0.018785639,0.0028614106,-0"
            + ".034323726,-0.014746001,-0.012633772,-0.02532035,0.0036237934,0.0026270852,-0.019551322,-0.0031254394,"
            + "0.006468702,0.023287328,0.021689955,-0.00047648922,0.010884582,-0.030284088,0.015115641,0.033214808,-0"
            + ".023260925,0.0059934505,-0.010013287,-0.0027425978,-0.04087164,-0.0066007166,-0.017359884,0.009188198,"
            + "-0.0085545285,-0.009201399,-0.003019828,0.008726147,-0.0015132143,-0.007953864,0.014561181,0"
            + ".018046359,0.01887805,0.0133400485,-0.003630394,-0.022152005,-0.010495139,-0.0045742965,0.02458107,-0"
            + ".001815197,-0.026204845,0.0017392888,0.023155313,0.012567764,-0.0132608395,0.0058086305,-0.005630411,0"
            + ".019168481,-0.009075985,0.012099113,-0.015036432,-0.019313697,-0.025095925,-0.008363108,0.009478629,-0"
            + ".01704305,-0.0018845046,0.0028944141,-0.011590858,-0.0006802864,0.016052943,-0.01040273,-0.031551424,"
            + "-0.001594073,-0.045993794,-0.00930701,0.00764363,0.0067558335,0.04670667,-0.0013671734,0.009571039,-0"
            + ".002679891,0.0014719598,-0.008580931,0.011689869,-0.009036381,-0.027036535,0.007122173,-0.031181784,-0"
            + ".0018085963,0.04044919,-0.015551289,-0.0029983756,-0.018904453,-0.015128843,0.01685823,0.0066436213,0"
            + ".009009978,0.00022751845,-0.010072693,-0.003445574,0.022046393,-0.013452261,0.0014331805,0.03234351,-0"
            + ".012534761,0.033742864,0.012911001,0.024198227,0.010864779,0.006528109,-0.009174996,-0.02091107,0"
            + ".004709611,0.021967186,0.011274024,-0.012567764,0.021227904,0.0006778111,0.005488496,-0.013650282,0"
            + ".031076174,-0.0146271875,0.005538001,-0.013307044,0.008145284,0.037544876,0.008831759,-0.008633737,-0"
            + ".014191541,-0.0018267484,0.007075968,0.02201999,0.018046359,0.005049548,-0.020052977,-0.018191574,0"
            + ".0005932394,0.0015767461,-0.008316902,-0.014957224,-0.02377578,0.0065710135,-0.00045132398,0"
            + ".012858196,-0.007663432,-0.018838445,0.0046006995,0.014851612,0.013300444,0.02819826,0.018349992,-0"
            + ".0008003369,0.012772387,-0.004838325,0.022072796,-0.004392777,0.0032261002,0.011300427,0.045254514,0"
            + ".000018358243,0.0067855367,-0.017518302,0.028488692,0.0011831784,-0.004650205,0.0030462306,-0"
            + ".010085895,-0.010013287,-0.028620707,-0.008653539,0.010429132,0.017557906,-0.019788949,-0.012508358,0"
            + ".02938639,0.023630565,0.00755122,-0.0018960559,-0.006871346,0.017676719,0.011663466,-0.014983627,-0"
            + ".014547979,-0.012950606,-0.019379703,0.008409313,-0.015630497,-0.017755928,0.019221287,-0.023379738,0"
            + ".005554503,0.003976932,-0.006844943,0.026825313,-0.028224664,-0.014508375,-0.03720164,-0.024369845,0"
            + ".019696537,0.01675262,0.01778233,0.02633686,0.0031072872,0.014112332,0.0006584215,-0.002432364,0"
            + ".0174919,-0.006924152,0.0016138752,0.030917756,0.0015156895,0.017557906,0.0088713635,-0.028251067,-0"
            + ".00039439282,0.013274041,0.017254272,-0.018310389,-0.0035148815,0.008488522,0.032713152,-0.0022112401,"
            + "-0.024765888,-0.00755122,-0.0026501878,-0.018825244,0.0041485503,0.005607309,-0.013003412,-0.03556466,"
            + "0.023709774,0.0076898346,-0.015696503,-0.012013304,0.010627153,0.01298361,0.005369683,-0.03825775,-0"
            + ".0008230269,0.02745898,0.082060106,0.013432458,-0.007108972,0.008646939,0.029465599,-0.011102405,-0"
            + ".027485384,-0.009069385,-0.005600708,-0.005013244,-0.01695064,0.00045091144,0.0064522005,0.002506622,"
            + "-0.013676684,-0.005970348,-0.0099472795,0.0026105833,-0.008396111,-0.009062784,0.0024389648,0"
            + ".00985487,0.00017460958,-0.007201382,0.03923466,-0.006376292,0.01889125,0.041003652,0.029544808,-0"
            + ".015274058,-0.00967005,-0.0018861548,0.0035049806,-0.010455535,-0.011379635,-0.0011625512,0.033188403,"
            + "0.018165171,0.010930787,-0.0051848628,0.004752516,0.024792291,0.011947297,-0.0074390075,-0.0088449605,"
            + "-0.006392794,-0.0012722882,0.030468907,-0.010085895,-0.015788915,0.026086032,-0.0013407705,-0"
            + ".014112332,-0.008594133,0.019379703,0.0068515437,-0.026363263,0.011848286,-0.033558045,-0.003851518,0"
            + ".009069385,-0.009155194,-0.009201399,-0.0025181733,-0.013221235,-0.04301027,-0.011485247,0.0033383125,"
            + "-0.017650316,-0.00672283,0.022033192,0.00022029891,-0.010528143,0.00060272793,0.029043153,0.010666758,"
            + "-0.013181631,0.0020759255,0.004686509,0.015340066,0.0052541704,-0.032554734,0.007174979,-0.007174979,"
            + "-0.008026471,0.017373087,0.020739451,-0.007940662,0.0050924527,0.022270817,0.008541327,0.024211429,0"
            + ".007168378,0.004864728,-0.0012178322,0.0051518595,0.0144687705,0.012884599,0.0061089634,-0.01059415,-0"
            + ".0134984655,0.024937507,-0.005234368,-0.015049634,0.002311901,0.003805313,-0.012653573,-0.0008106505,0"
            + ".0026056329,-0.029650418,0.04844926,0.00008384973,-0.01335985,-0.019406106,0.011491847,0.02525434,-0"
            + ".008283899,-0.0013712989,-0.024237832,-0.017729525,-0.034772575,-0.021610746,0.013056218,0.006485204,"
            + "-0.0044224802,0.014389562,0.012600768,-0.029597614,-0.017795531,-0.022363227,-0.0042013563,-0"
            + ".00433007,-0.022970494,0.0032624041,-0.018587617,-0.017993553,-0.0033762665,0.021676753,0.01225753,-0"
            + ".013320246,-0.023089306,0.018006755,-0.022785673,-0.011432441,0.004188155,-0.027881427,-0.0071881805,"
            + "-0.022072796,-0.010303719,0.009953881,0.018534811,0.0033861676,0.0037789103,0.01639618,0.003693101,-0"
            + ".02782862,-0.015696503,-0.0049505373,0.016699813,0.032290705,0.030020058,-0.014336756,-0.008693144,0"
            + ".015617295,-0.005445591,-0.0017673419,-0.0018432501,-0.0105017405,0.009505032,0.0147856055,0.03160423,"
            + "0.014904418,-0.00396043,0.00598685,0.022350026,-0.006003352,0.0046931095,-0.032211497,-0.013531469,-0"
            + ".029043153,0.0285679,-0.022996897,-0.018455604,0.02017179,0.020686645,0.0029719726,0.04359113,0"
            + ".0044752858,0.018548014,-0.0047030104,0.03519502,0.017095856,-0.0053564813,-0.01889125,-0.005739323,-0"
            + ".025003515,0.00023597562,-0.027643802,-0.030706534,0.02192758,-0.0003046643,0.010541344,0.02525434,0"
            + ".002569329,-0.004874629,0.016462186,-0.019881358,-0.010112298,-0.0021963886,-0.018957257,-0.0082905,-0"
            + ".007887856,-0.043195087,-0.021399522,-0.005141958,-0.023538155,-0.01262717,0.0058383336,-0.024752688,"
            + "-0.03572308,0.0010354874,-0.008330104,0.030680131,0.011069402,0.009049582,0.020977078,-0.013003412,-0"
            + ".0054290895,-0.013980318,0.00773604,-0.019564524,0.0018201476,0.032396317,0.0121057145,-0.038310558,-0"
            + ".0043564728,-0.0076502305,0.0117624765,-0.002072625,0.01631697,0.01279879,0.014217944,-0.022891285,-0"
            + ".008580931,-0.023709774,0.009709654,-0.006105663,0.020145386,-0.0026947425,-0.012699779,-0.0063795927,"
            + "0.037439264,-0.037333652,0.021161897,0.004670007,-0.010924186,-0.016792223,-0.029175168,-0.004557795,0"
            + ".012587567,0.0063795927,0.0033828672,-0.009128791,0.039762717,0.022917688,-0.010138701,0.001454633,0"
            + ".011742675,0.0071881805,0.02562398,0.022534847,-0.032211497,0.023590961,-0.014244346,0.011538053,-0"
            + ".03530063,-0.01354467,0.006844943,-0.017148662,-0.020515027,0.008759151,-0.0016650307,-0.015854921,-0"
            + ".017241072,0.012277333,-0.012600768,-0.012198124,-0.026548082,0.005326778,-0.010990193,-0.023208119,-0"
            + ".00023680071,0.0006023154,-0.010620553,0.018957257,-0.0020610737,0.0033746164,0.027247759,-0"
            + ".008547928,0.021742761,0.007049565,-0.008356507,-0.006772335,0.007194781,-0.020581035,-0.01861402,0"
            + ".012792189,-0.011986901,-0.02330053,-0.022970494,0.004874629,0.009168396,0.008785554,-0.00080446235,0"
            + ".008442316,0.007174979,0.023524953,-0.006336688,-0.01225753,0.0029356687,-0.023683371,-0.002617184,0"
            + ".030891353,-0.0073531982,0.007023162,0.031445812,0.013584275,-0.00783505,0.009788862,-0.008924169,-0"
            + ".03242272,0.024541464,-0.0119737,-0.015221252,-0.023815386,-0.007709637,-0.016739417,0.0019934163,0"
            + ".0037492071,-0.025808802,0.0156569,0.043036673,-0.0007768218,0.008805356,0.005353181,-0.02165035,-0"
            + ".034693368,0.009795464,0.0034092702,-0.011300427,-0.0037162034,0.011637064,-0.008396111,-0.03260754,-0"
            + ".008435716,-0.011650265,-0.039657105,-0.016158555,-0.030891353,-0.006798738,0.018363193,-0.017425891,"
            + "-0.011742675,0.0506671,-0.010990193,0.020581035,0.015590892,-0.009214601,-0.0014934121,0.010739366,0"
            + ".01713546,-0.0027855025,-0.010825175,-0.0041386494,0.007907659,0.0037690091,0.009775661,0.0006827616,0"
            + ".0040231366,0.019458912,-0.01510244,-0.014693195,-0.008580931,-0.008864762,0.022587651,-0.008719547,-0"
            + ".0043531726,0.008613935,0.014178339,-0.002782202,-0.009518233,0.0010627153,0.007762443,0.023841789,-0"
            + ".027775815,-0.009544636,-0.01262717,-0.012336739,0.006957155,0.014151936,-0.016224561,0.006891148,-0"
            + ".026099233,-0.021874774,-0.01372949,-0.00837631,-0.029175168,-0.03186826,0.014812008,-0.024726285,-0"
            + ".002356456,0.0013432459,0.016594201,0.0144423675,-0.015247655,-0.0156701,-0.0076766335,-0.017109057,-0"
            + ".006336688,0.0020742752,-0.009399421,-0.006650222,-0.004957138,0.011274024,0.0054587927,-0.008244295,"
            + "-0.00071700284,-0.022561248,-0.022112401,-0.0051287566,-0.022363227,-0.047868397,-0.011386236,-0"
            + ".020250998,-0.016224561,0.026231248,0.20298524,0.011643664,-0.0029521706,0.056185298,0.033188403,0"
            + ".004901032,0.010798772,0.01510244,-0.031049771,0.014191541,0.028752722,0.023419343,-0.018825244,-0"
            + ".006501706,-0.008145284,-0.04649545,-0.03305639,-0.021901177,-0.010250913,-0.03508941,-0.008726147,-0"
            + ".0043564728,0.003917525,-0.003445574,0.021769164,0.0075116153,-0.001519815,0.0010973691,0.026270851,-0"
            + ".008052874,-0.007993468,-0.009795464,0.017003445,0.0022590952,0.0067921374,0.0016732817,-0.00838291,-0"
            + ".009703053,0.017069453,-0.0008943971,-0.012369743,-0.013940713,0.010026488,-0.007148576,-0.020277401,0"
            + ".015247655,0.000884496,-0.0031023368,0.014930821,0.020858264,-0.02505632,-0.02542596,0.016581,0"
            + ".016990244,0.009551237,0.021478731,0.014455569,0.0023201518,-0.023062903,-0.011386236,-0.011128808,0"
            + ".037069622,0.0013292193,0.028224664,-0.005683217,0.0029802236,-0.011254222,-0.0075050145,0.011643664,"
            + "-0.013505066,0.017148662,-0.02200679,-0.007571022,-0.004498388,-0.02274607,-0.0076832343,0.02790783,0"
            + ".01372949,0.009505032,0.03049531,-0.001925759,0.0004139887,-0.022178408,-0.00052846986,-0.013029815,-0"
            + ".027300565,0.021478731,-0.007432407,-0.007148576,0.0025016717,0.019445712,-0.02644247,-0.0005177437,-0"
            + ".013584275,0.015920928,0.007102371,-0.017993553,0.007201382,-0.010455535,-0.013280641,-0.0072541875,0"
            + ".01409913,-0.0063894936,0.0034983798,-0.035696674,-0.0012648624,0.004561095,0.0016831828,0.0033960687,"
            + "-0.017161863,-0.001694734,-0.01638298,-0.0230365,0.00018162285,0.008990176,0.038178544,-0.010283916,-0"
            + ".02091107,0.011109006,-0.0056898175,0.02533355,-0.002229392,-0.008957173,0.018059561,-0.03371646,-0"
            + ".012660175,0.0019571125,0.023828587,0.0060825604,-0.048211634,-0.0061485674,0.028251067,-0.0010478637,"
            + "0.021016682,-0.0069109504,-0.012264132,0.0004579247,0.018178374,-0.019128876,0.0041980557,-0"
            + ".025439162,-0.0043234695,0.012112315,-0.011953898,0.03382207,-0.010838376,0.0006811114,-0.0036435956,"
            + "-0.030680131,-0.035907898,-0.0022161906,-0.0023053002,-0.0070957704,0.032554734,0.019762546,-0"
            + ".02128071,-0.05137998,-0.019775746,0.0034488745,0.026772507,-0.0396043,0.0023086006,0.043485522,-0"
            + ".005204665,-0.01188129,-0.0074192053,-0.16602123,0.023168515,0.0076502305,-0.009128791,0.013148627,0"
            + ".005600708,0.02901675,0.010937387,0.00064150715,0.0007083394,0.023445746,0.0060462565,-0.0060462565,-0"
            + ".023894593,-0.00045132398,-0.012297135,-0.024264233,0.008693144,0.012369743,0.0004123385,0.008686543,0"
            + ".011194815,0.023696572,-0.021439128,0.0014744351,0.014349958,0.034666963,0.013663483,0.01759751,0"
            + ".009531435,0.011755876,-0.0009645297,0.015604094,0.007174979,0.0072475867,-0.0018250982,-0.0022887986,"
            + "-0.002681541,-0.0048845303,0.012382944,0.009095787,-0.01906287,0.017373087,-0.0021864874,0.0022310421,"
            + "0.0037294049,-0.0029901245,-0.046733074,-0.0010519892,-0.01815197,0.031260993,-0.037835307,-0"
            + ".034138907,0.015326864,-0.010297118,0.0044554835,0.003554486,0.01391431,0.0019092573,0.0052211666,-0"
            + ".012369743,-0.014363159,0.00874595,0.009729456,-0.033584446,-0.019841755,-0.0004967039,-0.0009843318,"
            + "-0.014402764,0.0043069674,-0.027934233,0.008963773,0.0072079827,0.024290636,0.005277273,0.02340614,-0"
            + ".017650316,-0.0090561835,0.027802218,-0.0070693674,-0.012673376,-0.021043085,-0.056132495,0.02790783,"
            + "-0.026825313,0.011214618,-0.015709706,-0.009835068,0.0044224802,0.0019356601,0.029676821,-0.006594116,"
            + "-0.022614054,-0.0028449087,0.0033201603,0.034191713,0.0033696657,0.0008902716,-0.009201399,-0"
            + ".008864762,0.013148627,0.006495105,-0.022891285,0.024818694,0.011274024,0.033135597,-0.007577623,0"
            + ".0039670304,0.017214669,-0.028515095,-0.01695064,0.009498431,0.021254307,0.025175134,0.0057096197,0"
            + ".027643802,0.004181554,-0.013861504,0.034138907,-0.013821901,0.01594733,0.0060924613,0.0002652663,0"
            + ".0147856055,-0.0033350121,-0.009953881,-0.1077237,-0.022072796,0.010151902,0.02340614,-0.035168618,0"
            + ".007293792,-0.011465444,0.004577597,-0.012468753,0.008699744,0.01206611,-0.033452433,0.013452261,-0"
            + ".020132186,0.022336826,-0.007102371,-0.030653728,-0.01611895,-0.025637183,-0.005174962,-0.0021402824,"
            + "-0.018442402,-0.0017359884,0.0040792427,0.012000103,-0.026323657,-0.005333379,-0.0029241175,0"
            + ".021676753,0.022442436,0.0018085963,-0.0059571466,0.008303702,-0.055762853,0.00847532,0.030548116,-0"
            + ".015683303,0.006316886,0.010613952,-0.0034125706,-0.0034521748,-0.009610644,0.009973682,-0.027432578,0"
            + ".027855024,-0.023683371,-0.025412759,0.005729422,0.005511598,-0.018495208,-0.016356576,0.007056166,-0"
            + ".014072727,-0.0023630566,0.021346718,0.01786154,0.024765888,0.0002749611,-0.021425925,-0.015802115,-0"
            + ".03160423,-0.017214669,0.0004111009,0.012732782,0.018402798,0.019894559,-0.018217977,-0.009062784,-0"
            + ".0010676659,-0.032290705,0.0041980557,-0.013676684,-0.0130232135,0.009346615,-0.01639618,0.006917551,"
            + "-0.013419257,-0.027274162,0.008897766,0.019683337,-0.0078086476,-0.0313138,-0.012415948,-0.016079346,0"
            + ".01391431,-0.003141941,-0.012884599,0.013861504,0.005571005,-0.043696743,-0.008508324,0.018191574,0"
            + ".014746001,0.00819809,-0.031102577,0.0015288909,0.0025924314,0.0030973863,0.04427761,-0.034904588,-0"
            + ".015802115,-0.004871329,-0.06305005,0.027643802,0.004716212,0.013689886,0.0037558076,-0.03279236,0"
            + ".0033663656,-0.00016821513,0.029043153,0.018666826,-0.018812042,0.006366391,-0.005633712,-0.014429166,"
            + "-0.012693178,-0.032924373,0.02007938,0.0052277674,0.024198227,0.02154474,0.0148912165,0.005604008,-0"
            + ".006405995,0.016805425,0.0037228041,0.018732833,-0.0312874,0.023736177,-0.0025099225,-0.015920928,0"
            + ".014138735,-0.008052874,0.013782296,0.011472045,-0.025755996,0.016145352,-0.024185026,0.003950529,0"
            + ".01638298,-0.01391431,-0.009828467,0.00414525,0.027353369,-0.012693178,-0.017689921,-0.023168515,-0"
            + ".027115744,0.009722856,0.030627325,0.00027227955,0.04578257,0.01225753,0.006392794,-0.027115744,-0"
            + ".005656814,-0.010646956,0.0146535905,0.01639618,-0.000047803627,-0.0071287737,0.018442402,-0"
            + ".001787144,-0.00016336773,-0.01639618,0.0050363466,0.011590858,-0.023815386,-0.013109023,0.012224527,"
            + "-0.03057452,-0.018006755,0.0082905,0.012825192,-0.012191524,0.023287328,-0.015617295,-0.0011741024,-0"
            + ".015432475,0.002699693,0.03437653,-0.01602654,0.029043153,-0.014481972,0.046917893,0.007359799,0"
            + ".034033295,-0.019696537,0.014244346,-0.006531409,-0.012448952,-0.024831897,-0.005019845,0.0010206358,0"
            + ".0073267953,0.04541293,0.028066248,-0.012092513,0.0068185404,0.004151851,0.019155279,0.008217892,0"
            + ".0076568313,-0.0067393314,-0.03371646,-0.016871432,-0.002082526,-0.042059764,-0.04414559,-0.014257547,"
            + "0.02165035,-0.0027954034,-0.029835239,0.005699719,0.006620519,-0.030257685,0.04042279,-0.029597614,-0"
            + ".009524834,-0.0029406191,0.023049703,0.014481972,0.015960533,0.0062343767,0.012422549,0.009465427,0"
            + ".027221356,0.0087393485,-0.008297101,0.009049582,0.020422617,0.004495088,-0.0011476995,0.022152005,-0"
            + ".015089238,-0.0042508612,-0.02330053,0.026732903,0.058825586,-0.018653626,0.05840314,0.0047393143,0"
            + ".03276596,-0.0011460495,0.0032739553,0.009518233,0.0014868114,0.000076011376,0.020871466,0.0063036843,"
            + "-0.013597476,-0.005359782,0.034402933,-0.011016596,-0.009650247,0.021399522,-0.012950606,-0"
            + ".0130760195,-0.012567764,0.002938969,0.027274162,-0.009934079,-0.004521491,0.0026237848,-0.02000017,0"
            + ".0068317414,0.00967005,-0.0070891697,-0.0026485375,-0.02568999,0.013003412,0.01898366,-0.04448883,-0"
            + ".021663552,0.014534778,0.0013894509,-0.027062938,0.036171928,0.019419309,0.0066040168,0.0072343852,0"
            + ".025465565,-0.012316938,0.015155246,0.012356541,-0.037439264,0.0020330206,-0.049690194,-0.0174919]) as"
            + " l2_dist, n_tokens, combined from fineFoodReviews order by l2_dist ASC limit 10";
    printStatus(Color.YELLOW, "Search the most relevant review with the embedding of tomato soup");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.GREEN, "***************************************************");
    printStatus(Quickstart.Color.YELLOW, "Example query run completed.");
    printStatus(Quickstart.Color.GREEN, "***************************************************");
  }

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return MULTI_STAGE_TABLE_DIRECTORIES;
  }

  @Override
  protected Map<String, Object> getConfigOverrides() {
    Map<String, Object> configOverrides = new HashMap<>();
    configOverrides.put(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    return configOverrides;
  }

  @Override
  protected int getNumQuickstartRunnerServers() {
    return 3;
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "MULTI_STAGE"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
