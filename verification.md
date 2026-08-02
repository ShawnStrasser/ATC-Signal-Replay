# Source verification for the paper

Paper reviewed: `paper/manuscript.typ`  
Bibliography reviewed: `paper/references.bib`  
Source review date: 2026-08-01  
Corrections and requirements update: 2026-08-02

## Overall result

I independently checked all nine entries in `paper/references.bib` against the original publisher, government-repository, institutional, or standards source. I found no fabricated source, DOI, report number, author group, or title.

The sources are genuine and the principal related-work claims are supported. Two attribution issues found during review have been corrected in the manuscript:

1. Li et al. (2008) describes its own `CIDScript` language. XML scripting is now attributed separately to the Idaho report (Ahmed et al., 2010).
2. Tung (2012) now supports only the single NEMA TS2 Type-1 controller study. Tung (2015) separately supports the later evaluation of 20 NTCIP-based programs on five compliant controller models.

The DTW citation supports the mathematical foundation, not traffic-signal testing specifically. The traffic-event application, Jaccard cost, windowing, thresholds, and interpretation are the paper’s own method. The “literature reviewed did not identify...” sentence is a search conclusion and cannot be proven by any single bibliography entry; it should remain a cautious literature-review statement.

## How to interpret the verdicts

- **Supported** means the cited source says substantially what the paper attributes to it.
- **Supported with scope limit** means the source supports the underlying point, but the paper’s wording is broader than the source unless the stated qualification is kept.
- **Not a source-supported claim** means the statement is the author’s synthesis, method, or result rather than something established by that citation. This is not evidence of fabrication; it is a boundary on what the citation proves.

Page numbers below are PDF page numbers where a PDF is linked. For reports with front matter, the printed page number may differ from the PDF page number; search the quoted section heading or distinctive phrase if the viewer uses different numbering.

## Citation-to-claim summary

| Key | Source identity | Main manuscript claim checked | Result |
|---|---|---|---|
| `li2008` | Real 2008 journal article; DOI `10.1016/j.trc.2007.10.001` | Automated controller testing using a CID and a test-script language | Supported, but the paper uses `CIDScript`, not XML |
| `ahmed2010` | Real Idaho Transportation Department report FHWA-ID-10-180 | XML scripts activate inputs, control timing, and verify responses; NTCIP/TS1/TS2 scope | Supported |
| `tung2012` | Real Florida State University/FDOT report BDK83-977-08 | Earlier automated ASC testing system and NTCIP direction | Supported with scope limit |
| `tung2015` | Real FDOT/FSU report BDV30-977-05 | NTCIP-based tests across multiple controller models | Supported |
| `wang2021` | Real IEEE article; DOI `10.1109/MITS.2019.2898968` | Physical/virtual controller interfaces for HILS and simulation | Supported |
| `stevanovic2017` | Real FDOT report BDV27-977-06 | Six vendors/controllers, HILS, high-resolution event codes | Supported |
| `sakoe1978` | Real IEEE article; DOI `10.1109/TASSP.1978.1163055` | Monotone time-warping/dynamic-programming alignment | Supported as mathematical foundation |
| `sturdevant2012` | Real Purdue/INDOT data paper; DOI `10.4231/K4RN35SH` | High-resolution event enumerations and 100-ms resolution | Supported |
| `ntcip1202` | Real AASHTO/ITE/NEMA standard, v03B, published October 2023 | NTCIP objects for remote detector/preempt control and status | Supported; direct standard PDF is the best locator |

## Detailed verification

### 1. Li et al. (2008) — `li2008`

**Bibliography identity.** The [ScienceDirect record](https://www.sciencedirect.com/science/article/abs/pii/S0968090X07000812) and [University of Idaho record](https://verso.uidaho.edu/esploro/outputs/journalArticle/Design-of-traffic-controller-automated-testing/996630838201851) identify *Design of traffic controller automated testing tool* by Zhen Li, Ahmed Abdel-Rahim, Brian Johnson, and Michael Kyte, published in *Transportation Research Part C: Emerging Technologies*, volume 16, issue 3, pages 277–293, in 2008. The DOI resolves as `https://doi.org/10.1016/j.trc.2007.10.001`. These match the bibliography except for ordinary author-name formatting (`Ahmed-Abdel-Rahim` in the BibTeX entry versus `Ahmed Abdel-Rahim` in the source records).

**How to see the relevant text.**

1. Open the [publisher abstract](https://www.sciencedirect.com/science/article/abs/pii/S0968090X07000812).
2. Read the **Abstract**. It says the paper presents a traffic-controller automated-testing tool and a new script language, `CIDScript`, using controller-interface-device technology.
3. In the same abstract, read the three examples: the automated tester can test conditions unavailable to a standard suitcase tester, run tests faster/more efficiently, and document results for later comparison.
4. The [TRID record](https://trid.trb.org/View/863267) independently repeats the abstract and confirms the authors, journal, volume, issue, pages, publisher, and 2008 publication date.

**Claim assessment.** The manuscript’s statement that this work established automated controller testing is supported. The specific phrase “used XML scripts” is not supported by the publisher abstract: this paper names `CIDScript`. XML is directly documented in the separate Idaho report below. Treat the sentence at `paper/manuscript.typ:54` as a combined summary, with XML attributed to `ahmed2010`.

### 2. Ahmed et al. (2010) — `ahmed2010`

**Bibliography identity.** The official [ROSA P record](https://rosap.ntl.bts.gov/view/dot/23916) identifies *An automated testing tool for traffic signal controller functionalities*, report FHWA-ID-10-180, by Sk. Monsur Ahmed, Cody Browne, Ahmed Abdel-Rahim, and Richard Wall, prepared for the Idaho Transportation Department in 2010. The [official PDF](https://rosap.ntl.bts.gov/view/dot/23916/dot_23916_DS1.pdf) is 31 pages and its title page says March 2010.

**How to see the relevant text.**

1. Open the [official PDF](https://rosap.ntl.bts.gov/view/dot/23916/dot_23916_DS1.pdf).
2. On PDF page 2, read the **Abstract**. It explicitly says the tool uses XML script files to specify activated inputs, activation timing, and verification of controller responses. The same paragraph describes the limited NEMA TS1 firmware scope, a TS2 version, NTCIP over RS-232/Ethernet, and vendor-specific NTCIP interpretation differences.
3. On PDF page 10, read **Chapter 1 — Introduction / Background**. It explains that manual suitcase testing requires repeated manual command entry and that the project aimed to automate and standardize testing.
4. On printed report page 16 / PDF page 25–26, read **Chapter 3 — Traffic Controller Test Automation**. It says the tester covers vehicle-detector, pedestrian-button, and preemption inputs and that response validation is the tester’s responsibility.
5. On PDF page 26, search for `The automated testing uses XML script files`. The report says the XML specifies activated inputs, timing, and response verification; it also says tests can be repeated and results recorded automatically.
6. On PDF pages 27–28, read the communications/component description and **Predefined Tests**. It documents NTCIP over serial/Ethernet and 14 predefined tests covering ring, phase, and preemption parameters.

**Claim assessment.** Supported. This report directly supports the manuscript’s XML-script, input-timing, predefined-response, repeatability, NTCIP, and manual-testing comparison statements. It also supplies an important limitation: the software still required controller/firmware-specific verification.

### 3. Tung (2012) — `tung2012`

**Bibliography identity.** The official [ROSA P record](https://rosap.ntl.bts.gov/view/dot/25053) identifies Leonard J. Tung’s *Development of Automated Testing Tools for Traffic Control Signals and Devices*, final report dated 2012, Florida State University/Florida Department of Transportation, contract BDK83-977-08. The [official PDF](https://rosap.ntl.bts.gov/view/dot/25053/dot_25053_DS1.pdf) confirms the title, author, June 30, 2012 report date, and contract number on PDF page 2.

**How to see the relevant text.**

1. Open the [official PDF](https://rosap.ntl.bts.gov/view/dot/25053/dot_25053_DS1.pdf).
2. On PDF page 2, read the **Abstract**. It describes an automated testing system for a NEMA TS2 Type-1 actuated signal controller, 20 automated testing programs, an executable application, a user manual, and accompanying code/documents.
3. On PDF pages 9–10, read **I.1 Background** and the project scope. The report explains the difficulty of comprehensive manual controller testing and identifies NTCIP requirements as part of the testing context.
4. On PDF page 12, read **Table 1: Areas of Work and Scope**. It lists NTCIP 1202/1201/8007 review, system design, implementation, scripts, and testing/validation tasks.
5. On PDF pages 16–17, read **V. Conclusion**. The report says the system developed at that stage was still manufacturer-dependent because of differences in controller implementations, while NTCIP demonstrated the possibility of a manufacturer-independent system and additional work was needed.

**Claim assessment.** Supported with scope limit. Tung (2012) supports the existence of automated controller testing and the NTCIP direction, but it does not by itself establish the later cross-manufacturer result. Its conclusion expressly says additional work was needed and the tools were still manufacturer-dependent at that stage. Use Tung (2015) for the stronger multi-model claim.

### 4. Tung (2015) — `tung2015`

**Bibliography identity.** The official [ROSA P record](https://rosap.ntl.bts.gov/view/dot/28634) identifies Leonard J. Tung’s *Development of automated testing tools for traffic control signals and devices (NTCIP and Security) phase 2*, report BDV30-977-05, published in 2015 for FDOT/Florida State University. The [official PDF](https://rosap.ntl.bts.gov/view/dot/28634/dot_28634_DS1.pdf) confirms the report number and title on its documentation page.

**How to see the relevant text.**

1. Open the [official PDF](https://rosap.ntl.bts.gov/view/dot/28634/dot_28634_DS1.pdf).
2. On PDF page 2, read the **Abstract**. It says the project developed an NTCIP-based automated testing system for NTCIP-compliant ASCs, with 20 NTCIP-based programs covering ASC functionality.
3. On PDF page 4, read **Executive Summary**. It states that NTCIP made an NTCIP-based manufacturer-independent testing system possible and repeats the 20-program description.
4. On PDF page 10, read **II. Literature Review**. It lists NTCIP 1201, NTCIP 1202, and NTCIP 8007 as standards studied for the automated testing system.
5. On PDF page 12, read **Table 2: Results and Products**. It says testing was performed on five different models of NTCIP-compliant ASC from various manufacturers.
6. On PDF page 13, read **V. Conclusion**. It explicitly calls the developed NTCIP-based system manufacturer/vendor independent.

**Claim assessment.** Supported. This is the source that most directly supports the manuscript’s claim that Tung extended NTCIP-based automated testing across controller devices. It still describes functional/automated tests, not full-day field-log replay or automatic alignment of complete output traces.

### 5. Wang, Tian, and Yang (2021) — `wang2021`

**Bibliography identity.** The [TRID record](https://trid.trb.org/View/1855112) and the [DOI record](https://doi.org/10.1109/MITS.2019.2898968) identify *Virtual Controller Interface Device for Hardware-in-the-Loop Simulation of Traffic Signals* by Daobin Wang, Zongbei Tian, and Guangchuan Yang, in *IEEE Intelligent Transportation Systems Magazine*, volume 13, issue 2, pages 201–216. The issue publication year is 2021.

**How to see the relevant text.**

1. Open the [TRID record](https://trid.trb.org/View/1855112) or follow the DOI to IEEE/library access.
2. Read the abstract. It says traditional HILS uses a controller-interface device to connect a physical signal controller to simulation software.
3. Continue through the abstract. It says the paper develops and evaluates a virtual controller-interface device using NTCIP, and compares VCID-based HILS with emulator-in-the-loop, software-in-the-loop, and traditional HILS.
4. The abstract lists queue length, delay, and vehicle trajectories as performance measures and says the study evaluates real-time parameter updates through SNMP.

**Claim assessment.** Supported. The manuscript’s physical/virtual interface and controller/emulator-in-the-loop statement is a fair concise description of this source. The source concerns simulation and HILS performance, not field-log replay or software-version regression across production configurations; that distinction in the manuscript is accurate as a scope distinction, not a direct quote.

### 6. Stevanovic, Klanac, and Radivojevic (2017) — `stevanovic2017`

**Bibliography identity.** The official [ROSA P record](https://rosap.ntl.bts.gov/view/dot/31807) identifies *Development of minimum standards for event-based data collection loggers and performance measure definitions for signalized intersections* by Aleksandar Stevanovic, Ivica Klanac, and Danilo Radivojevic, published in 2017 for FDOT, contract BDV27-977-06. The [TRID record](https://trid.trb.org/View/1450305) confirms the final report, 305-page length, authors, date, and contract number. The [FDOT one-page summary](https://fdotwww.blob.core.windows.net/sitefinity/docs/default-source/research/reports/fdot-bdv27-977-06-sum.pdf) is an accessible official summary.

**How to see the relevant text.**

1. Open the [ROSA P record](https://rosap.ntl.bts.gov/view/dot/31807) and read its **Abstract**, or open the [TRID record](https://trid.trb.org/View/1450305) and read lines/paragraphs 31–35 in the rendered page.
2. The abstract says the study investigated six different controllers from six vendors connected simultaneously in a HILS setup.
3. The next paragraph says the first objective was minimum standards for event-based high-resolution controllers and that mandatory and optional event codes were determined.
4. The following paragraph says the second objective was retrieval and consistency of performance measures across controllers, including configuration, high-resolution data retrieval, and FDOT performance-measure tooling.
5. For an especially easy page-level check, open the [FDOT summary](https://fdotwww.blob.core.windows.net/sitefinity/docs/default-source/research/reports/fdot-bdv27-977-06-sum.pdf): PDF page 1, **Research Objectives** says six controllers were investigated; **Project Activities** says they were tested using HILS, separately and simultaneously; the next paragraph says a minimum set of event-based codes was identified.

**Claim assessment.** Supported. The source supports the six-vendor/controller, HILS, event-code, and high-resolution logging claims. It does not report the paper’s field-log replay or software-regression experiment, so the manuscript’s distinction from that work is appropriately cautious.

### 7. Sakoe and Chiba (1978) — `sakoe1978`

**Bibliography identity.** The [DOI](https://doi.org/10.1109/TASSP.1978.1163055) identifies *Dynamic Programming Algorithm Optimization for Spoken Word Recognition* by Hiroaki Sakoe and Seibi Chiba, *IEEE Transactions on Acoustics, Speech, and Signal Processing*, volume 26, issue 1, pages 43–49, 1978. An accessible copy of the [published seven-page paper](https://jeffe.cs.illinois.edu/teaching/compgeom/2022/refs/Sakoe-Chiba-DTW.pdf) is available for page-level inspection.

**How to see the relevant text.**

1. Open the [accessible paper copy](https://jeffe.cs.illinois.edu/teaching/compgeom/2022/refs/Sakoe-Chiba-DTW.pdf). Use the DOI for the authoritative bibliographic identity.
2. On printed page 43 / PDF page 1, read the **Abstract** and **Introduction**. The paper describes dynamic-programming time normalization, nonlinear warping, and minimized residual distance under timing variation.
3. On printed pages 43–44 / PDF pages 1–2, read **II. DP-Matching Principle**. The paper defines a warping function mapping one sequence’s time axis to another.
4. On printed page 44 / PDF page 2, read **B. Restrictions on Warping Function**. It explicitly gives monotonicity and continuity conditions for the warping path.
5. On printed page 46 / PDF page 4, read **III-A. DP-Equation**. It gives the dynamic-programming recurrence and the time-normalized distance calculation.

**Claim assessment.** Supported as a mathematical foundation. The paper is about speech, not signal controllers, and it does not support the manuscript’s Jaccard event-set cost or the paper’s thresholds. Those are the author’s adaptation of DTW.

### 8. Sturdevant et al. (2012) — `sturdevant2012`

**Bibliography identity.** The [Purdue e-Pubs record](https://docs.lib.purdue.edu/jtrpdata/3/) identifies *Indiana Traffic Signal Hi Resolution Data Logger Enumerations*, dated November 2012, by James R. Sturdevant and the listed INDOT, Purdue, and vendor coauthors, with DOI `10.4231/K4RN35SH`. An accessible [PDF copy](https://www.cflsmartroads.com/projects/smartsignals/ATSPM%20Detector%20Codes.pdf) carries the Purdue e-Pubs title page and recommended citation.

**How to see the relevant text.**

1. Open the [Purdue record](https://docs.lib.purdue.edu/jtrpdata/3/) to verify the title, date, DOI, authors, and recommended citation.
2. Open the [PDF copy](https://www.cflsmartroads.com/projects/smartsignals/ATSPM%20Detector%20Codes.pdf).
3. On PDF page 3, read the **Abstract**. It says the document defines enumerations for events recorded by traffic-signal controllers with high-resolution data loggers and gives a time resolution of the nearest 100 milliseconds.
4. On PDF pages 4–12, inspect the event-code tables. They define phase, pedestrian, overlap, detector, preemption, coordination, and cabinet/system events with parameters and descriptions.
5. For a direct match to the manuscript’s replay mapping, use PDF page 8 for detector codes 81/82 and pedestrian detector codes 89/90, and PDF page 9 for preemption codes 102/104. These are the same event-code families shown in Table 1 of the manuscript.

**Claim assessment.** Supported. This source directly supports the tenth-second/high-resolution event-record statement and the event-code substrate. It is an enumeration/data-definition document, not an automated testing or regression-testing study.

### 9. NTCIP 1202 v03B (2023) — `ntcip1202`

**Bibliography identity.** The official [NTCIP document-status page](https://www.ntcip.org/document-numbers-and-status/) lists NTCIP 1202 v03B as a published standard from October 2023. The [direct official PDF](https://www.ntcip.org/file/2023/11/NTCIP-1202v03.35e-aspublished.pdf) is titled *Object Definitions for Actuated Signal Controllers (ASC) Interface*, identifies AASHTO, ITE, and NEMA as publishers, and says it was published in October 2023. The bibliography’s title, organizations, year, version, and official landing-page URL are therefore genuine.

**How to see the relevant text.**

1. Open the [official PDF](https://www.ntcip.org/file/2023/11/NTCIP-1202v03.35e-aspublished.pdf), not only the bibliography’s general [document-status page](https://www.ntcip.org/document-numbers-and-status/).
2. On PDF page 8 / printed foreword page iii, read the **Foreword**. It says NTCIP 1202 defines how a management station interfaces with a field device to control and monitor traffic signal controllers and associated detectors, and that the data is defined in SNMP object-type format.
3. On printed page 169–170 / PDF pages 220–221, read sections **3.5.3.2.1** and **3.5.3.2.2**. These define monitoring vehicle-detector and pedestrian-detector status groups, with eight detectors per group.
4. On printed page 291 / PDF page 342, read section **5.3.11.3, Vehicle Detector Control Group Actuation**. It says a remote entity can place detector actuations through a read-write object, with one bit per detector.
5. On printed page 292 / PDF page 343, read section **5.3.12.2, Pedestrian Detector Control Group Actuation**. It gives the corresponding remote pedestrian-detector actuation object.
6. On printed pages 348–349 / PDF pages 399–400, read sections **5.7.3, Preempt Control Table**, and **5.7.3.2, Preempt Control State**. The standard says the control objects allow preempts to be activated remotely and that the read-write state turns the associated preempt actions on or off.
7. The standard’s object definitions are the authoritative place to verify the protocol claim; controller-specific support and vendor behavior still need separate validation.

**Claim assessment.** Supported. NTCIP 1202 directly supports the manuscript’s statement that vehicle, pedestrian, and preempt input states can be represented through NTCIP objects and remotely controlled. The standard does not, by itself, verify that MAXTIME or any particular vendor implements every object correctly; that is an implementation/evaluation claim made elsewhere in the manuscript.

## Claims not established by the bibliography alone

These are not fabricated citations, but they are claims that require the paper’s own evidence, code, data, or a documented literature-search method rather than the nine external sources:

- The 25 configurations, 23-hour traces, event counts, DTW scores, thresholds, 11/14 detections, and zero conflicts are empirical claims about this project. They should be checked against the repository evidence identified in `paper/EVIDENCE.md`.
- The MAXTIME version numbers, rail-preemption bug fix, overlap-clearance findings, and ODOT operational-use statement are project-specific claims, not claims established by the cited literature.
- The novelty statement that no prior study combined all of these elements is a review conclusion. To make it independently auditable, preserve the search dates, databases, search strings, inclusion/exclusion decisions, and any screened results.
- The manuscript’s statement that the replay “sends SNMP SET operations” is partly supported by NTCIP 1202’s read-write object definitions, but the actual MAXTIME/package implementation must be verified in the repository code and run evidence. The standard is not evidence that the project executed those calls.

## Actions applied on 2026-08-02

The repository paper was updated in response to this verification:

- The related-work paragraph now attributes `CIDScript` to Li et al. (2008) and XML scripts to Ahmed et al. (2010).
- The Tung discussion now distinguishes the 2012 single-controller work from the 2015 evaluation of 20 NTCIP-based programs on five compliant controller models.
- The Li, Wang, and Sakoe and Chiba article titles now use Chicago sentence case.
- Ahmed Abdel-Rahim's name is entered consistently with the source record.
- The NTCIP entry now uses the official title *Object Definitions for Actuated Signal Controllers (ASC) Interface*.
- The selected DTW explanation is the timeline figure; the alternative panels were removed.

## Updated TRB requirements review

The official author instructions and the author's local `2027-TRB-Annual-Meeting-Paper-Submission-Checklist.pdf` were reviewed. The rebuilt manuscript is 11 pages and meets the document requirements that can be checked locally: US Letter, one-inch margins, Times New Roman at 10 pt or larger, single spacing, one column, line numbers restarting on each page, bottom-centered page numbers, a separate structured abstract under 300 words, embedded figures and tables, no appendix, and Chicago author-date citations.

Submission-system actions remain the author's responsibility: paste the identical structured abstract into Editorial Manager, enter the single-author metadata, select the submission type and topic, provide the AI disclosure, and confirm the paper is not published or under review elsewhere if choosing Presentation and Publication. Any ODOT approval is required only if ODOT policy requires it.

## Time-sensitive submission status

The official TRB instructions state that the 2027 submission site closed August 1. On August 2, the Editorial Manager page displayed: “Site under development. Do not use for live manuscript submission.” If the manuscript was not already submitted, contact `TRBAMPapers@nas.edu` immediately to ask whether an extension or late-submission path exists. Do not assume that the visible “Submit a Manuscript” link accepts a valid live submission.

## Bottom line

The bibliography is real and traceable, and the citation-scope issues identified in this review have been corrected. The literature supports the manuscript's background claims within the limits documented above. Project-specific results remain supported by the repository evidence cataloged in `paper/EVIDENCE.md` and require the author's final review.