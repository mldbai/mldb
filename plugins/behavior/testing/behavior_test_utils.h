/* -*- C++ -*- */
// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#if 0
template<int start, int end>
struct DoPrintElements {

    template<typename... Args>
    static std::ostream & print(std::ostream & stream,
                                std::tuple<Args...> & tup)
    {
        return stream << std::get<start>(tup)
                      << (start == end - 1 ? "" : ",")
                      << DoPrintElements<start + 1, end>::print(stream, tup);
    }
};

template<int x>
struct DoPrintElements<x, x> {

    template<typename... Args>
    static std::ostream & print(std::ostream & stream,
                                std::tuple<Args...> & tup)
    {
        return stream;
    }
                                
};


template<typename... Args>
std::ostream & operator << (std::ostream & stream,
                            const std::tuple<Args...> & tup)
{
    return stream << "("
                  << DoPrintElements<0, sizeof...(Args)>::print(stream, tup)
                  << ")";
}

#endif

void testIntegrity(BehaviorDomain & behs)
{
    cerr << "testing integrity of " << MLDB::type_name(behs) << endl;

    behs.verify();

    uint32_t subjectCount = 0;

    // Check that subject behaviors are sorted properly
    auto onSubject = [&] (SH subject, const SubjectIterInfo & info)
        {
            ++subjectCount;
            auto prev = std::make_tuple(BH(0), Date(), 0);

            uint32_t totalBehs = 0;
            uint32_t totalCount = 0;
            Date earliest = Date::positiveInfinity();
            Date latest = Date::negativeInfinity();

            std::set<BH> allBehs;

            auto range = behs.getSubjectTimestampRange(subject);

            auto onBeh = [&] (BH beh, Date ts, uint32_t count)
            {
                //cerr << ts.print(6) << " " << beh << " " << count << endl;
                BOOST_CHECK(ts.isADate());

                if (!earliest.isADate())
                    earliest = ts;
                else BOOST_CHECK_GE(ts, earliest);

                latest = ts;

                BOOST_REQUIRE_GE(ts, std::get<1>(prev));
                BOOST_REQUIRE_GT(count, 0);
                if (ts == std::get<1>(prev)) {
                    BOOST_REQUIRE_GT(beh, std::get<0>(prev));
                }
                prev = std::make_tuple(beh, ts, count);

                allBehs.insert(beh);
                totalBehs += 1;
                totalCount += 1;

                return true;
            };
            
            //cerr << endl;
    
            behs.forEachSubjectBehaviorHash
                (subject, onBeh,
                 BehaviorDomain::SubjectBehaviorFilter(),
                 INORDER);

            // Check against the real stats
            auto stats = behs.getSubjectStats(subject, true);
            
            BOOST_CHECK_EQUAL(stats.numDistinctBehaviors, allBehs.size());
            BOOST_CHECK_EQUAL(stats.earliest, earliest);
            BOOST_CHECK_EQUAL(stats.numBehaviors, totalBehs);

            BOOST_CHECK_EQUAL(behs.knownSubject(subject), true);

            if (totalCount) {
                BOOST_CHECK_EQUAL(earliest, range.first);
                BOOST_CHECK_EQUAL(latest, range.second);
            }

            return true;
        };

    behs.forEachSubject(onSubject);

    auto onBehavior = [&] (BH beh, BehaviorIterInfo)
        {
            auto s1 = behs.getBehaviorStats(beh, BehaviorDomain::BS_ALL);
            
            auto sht1 = behs.getSubjectHashesAndTimestamps(beh, SH::max(), true);
            BOOST_CHECK_EQUAL(sht1.size(), s1.subjectCount);
            
            // Get non-sorted
            auto sht1a = behs.getSubjectHashesAndTimestamps(beh, SH::max(), false);
            
            BOOST_CHECK_EQUAL(sht1a.size(), s1.subjectCount);
            
            // Sort to check they're good
            std::sort(sht1a.begin(), sht1a.end());
            
            BOOST_CHECK_EQUAL_COLLECTIONS(sht1.begin(), sht1.end(),
                                          sht1a.begin(), sht1a.end());

            std::map<SH, Date> timestamps;
            
            int index = 0;
            auto onSubject1 = [&] (SH subject, Date ts)
            {
                BOOST_REQUIRE_LT(index, sht1.size());
                BOOST_CHECK_EQUAL(subject, sht1[index].first);
                BOOST_CHECK_EQUAL(ts, sht1[index].second);

                BOOST_CHECK(timestamps.insert(make_pair(subject, ts)).second);

                ++index;
                return true;
            };
            
            behs.forEachBehaviorSubject(beh, onSubject1, true, INORDER, SH::max());

            index = 0;
            auto onSubject2 = [&] (SH subject, Date ts)
            {
                BOOST_REQUIRE_LT(index, sht1.size());
                BOOST_CHECK_EQUAL(subject, sht1[index].first);
                ++index;
                return true;
            };

            behs.forEachBehaviorSubject(beh, onSubject2, false, INORDER, SH::max());

            std::set<SH> doneSubjects;

            auto onSubject3 = [&] (SH subject, Date ts)
            {
                BOOST_CHECK(timestamps.count(subject));
                BOOST_CHECK(!doneSubjects.count(subject));
                doneSubjects.insert(subject);
                BOOST_CHECK_EQUAL(timestamps[subject], ts);
                return true;
            };

            behs.forEachBehaviorSubject(beh, onSubject3, true, ANYORDER, SH::max());
            BOOST_CHECK_EQUAL(doneSubjects.size(), timestamps.size());
            
            doneSubjects.clear();
            auto onSubject4 = [&] (SH subject, Date ts)
            {
                BOOST_CHECK(timestamps.count(subject));
                doneSubjects.insert(subject);
                return true;
            };

            behs.forEachBehaviorSubject(beh, onSubject4, false, ANYORDER, SH::max());
            BOOST_CHECK_EQUAL(doneSubjects.size(), timestamps.size());

            return true;
        };

    behs.forEachBehavior(onBehavior);

    // Check that getting info for an invalid subject hash doesn't throw
    SH hash(10298291381ULL);
    BOOST_CHECK_EQUAL(behs.knownSubject(hash), false);
    BOOST_CHECK_NO_THROW(behs.getSubjectStats(hash));

    auto allMd = behs.getAllFileMetadata();

    for (auto it = allMd.begin(), end = allMd.end();  it != end;  ++it)
        BOOST_CHECK_EQUAL(behs.getFileMetadata(it.memberName()), *it);

    //Make sure we can loop over the subjects
    auto subjectStream = behs.getSubjectStream(0);
    for ( uint32_t i = 0; i < subjectCount; ++i )
    {
        auto subjectHash = subjectStream->next();
        auto subjectId = behs.getSubjectId(subjectHash);
        BOOST_CHECK_EQUAL(subjectId != Id(), true);
    }

     //Make sure we can start somewhere in the middle
    if (subjectCount > 1)
    {
        uint32_t start = subjectCount / 2;
        auto subjectStream = behs.getSubjectStream(start);
        for ( uint32_t i = start; i < subjectCount; ++i )
        {
            auto subjectHash = subjectStream->next();
            auto subjectId = behs.getSubjectId(subjectHash);
            BOOST_CHECK_EQUAL(subjectId != Id(), true);
        }
    }    
}

void testEquivalent(BehaviorDomain & behs1,
                    BehaviorDomain & behs2)
{
    // Make sure the set of behaviors is the same
    vector<BH> allBehs1 = behs1.allBehaviorHashes(true /* sorted */);
    vector<BH> allBehs2 = behs2.allBehaviorHashes(true /* sorted */);

    BOOST_CHECK_EQUAL_COLLECTIONS(allBehs1.begin(), allBehs1.end(),
                                  allBehs2.begin(), allBehs2.end());

    // Make sure the set of subjects is the same
    vector<SH> allSubjects1 = behs1.allSubjectHashes(SH::max(), true);
    vector<SH> allSubjects2 = behs2.allSubjectHashes(SH::max(), true);

    BOOST_CHECK_EQUAL_COLLECTIONS(allSubjects1.begin(), allSubjects1.end(),
                                  allSubjects2.begin(), allSubjects2.end());
    
    // Make sure behaviors are equal for each subject
    for (auto it = allSubjects1.begin(), end = allSubjects1.end();
         it != end;  ++it) {
        SH subj = *it;

        if (behs1.hasSubjectIds && behs2.hasSubjectIds)
            BOOST_CHECK_EQUAL(behs1.getSubjectId(subj),
                              behs2.getSubjectId(subj));

        auto stats1 = behs1.getSubjectStats(subj, true);
        auto stats2 = behs2.getSubjectStats(subj, true);

        BOOST_CHECK_EQUAL(stats1.earliest, stats2.earliest);
        BOOST_CHECK_EQUAL(stats1.numDistinctBehaviors,
                          stats2.numDistinctBehaviors);
        BOOST_CHECK_EQUAL(stats1.numBehaviors,
                          stats2.numBehaviors);

        vector<std::tuple<BH, Date, uint32_t> > behs;
        auto onBeh1 = [&] (BH beh, Date ts, uint32_t count)
            {
                behs.push_back(std::make_tuple(beh, ts, count));
                return true;
            };

        behs1.forEachSubjectBehaviorHash
            (subj, onBeh1,
             BehaviorDomain::SubjectBehaviorFilter(),
             INORDER);

        int i = 0;
        auto onBeh2 = [&] (BH beh, Date ts, uint32_t count)
            {
                BOOST_REQUIRE(i < behs.size());
                BOOST_CHECK_EQUAL(std::get<0>(behs[i]), beh);
                BOOST_CHECK_EQUAL(std::get<1>(behs[i]), ts);
                BOOST_CHECK_EQUAL(std::get<2>(behs[i]), count);
                ++i;
                return true;
            };
        
        behs2.forEachSubjectBehaviorHash
            (subj, onBeh2,
             BehaviorDomain::SubjectBehaviorFilter(),
             INORDER);

        BOOST_CHECK_EQUAL(i, behs.size());
#if 0
        if (info1.behaviors.size() != info2.behaviors.size()) {
            for (unsigned i = 0;
                 i < info1.behaviors.size()
                     && i < info2.behaviors.size();  ++i) {

                auto b1 = info1.behaviors[i];
                auto b2 = info2.behaviors[i];

                cerr << i << ": " << b1.timestamp << "/"
                     << b2.timestamp << " " << BH(b1.behavior) << "/"
                     << BH(b2.behavior)
                     << " " << b1.count << "/" << b2.count << endl;
            }
        }
#endif
    }

    // Make sure behavior info is the same
    for (auto it = allBehs1.begin(), end = allBehs1.end();
         it != end;  ++it) {

        BH beh = *it;

        BOOST_CHECK_EQUAL(behs1.getBehaviorId(beh),
                          behs2.getBehaviorId(beh));

        auto s1 = behs1.getBehaviorStats(beh, BehaviorDomain::BS_ALL);
        auto s2 = behs2.getBehaviorStats(beh, BehaviorDomain::BS_ALL);
        
        BOOST_CHECK_NE(s1.id, Id());
        BOOST_CHECK_EQUAL(s1.id, s2.id);
        BOOST_CHECK_EQUAL(s1.subjectCount, s2.subjectCount);
        BOOST_CHECK_EQUAL(s1.earliest, s2.earliest);
        BOOST_CHECK_EQUAL(s1.latest, s2.latest);

        auto sht1 = behs1.getSubjectHashesAndTimestamps(beh, SH::max(), true);

        BOOST_CHECK_EQUAL(sht1.size(), s1.subjectCount);
        
        // Get non-sorted
        auto sht1a = behs1.getSubjectHashesAndTimestamps(beh, SH::max(), false);

        BOOST_CHECK_EQUAL(sht1a.size(), s1.subjectCount);

        // Sort to check they're good
        std::sort(sht1a.begin(), sht1a.end());

        BOOST_CHECK_EQUAL_COLLECTIONS(sht1.begin(), sht1.end(),
                                      sht1a.begin(), sht1a.end());
        

        auto sht2 = behs2.getSubjectHashesAndTimestamps(beh, SH::max(), true);

        BOOST_CHECK_EQUAL_COLLECTIONS(sht1.begin(), sht1.end(),
                                      sht2.begin(), sht2.end());

        // Check that all timestamps are correct
        auto shat1 = behs1.getSubjectHashesAndAllTimestamps(beh, SH::max(), true);
        auto shat2 = behs2.getSubjectHashesAndAllTimestamps(beh, SH::max(), true);

        BOOST_CHECK_EQUAL_COLLECTIONS(shat1.begin(), shat1.end(),
                                      shat2.begin(), shat2.end());

        std::map<SH, Date> subjs;
        for (auto & s: shat1)
            subjs.insert(s);

        vector<pair<SH, Date> > sht3(subjs.begin(), subjs.end());

        BOOST_CHECK_EQUAL_COLLECTIONS(sht1.begin(), sht1.end(),
                                      sht3.begin(), sht3.end());
    }

    BOOST_CHECK_EQUAL(behs1.getAllFileMetadata().toString(),
                      behs2.getAllFileMetadata().toString());
}

template<typename Dom>
void createTestBehaviors(const std::shared_ptr<Dom>& mut)
{
    mut->setFileMetadata("hello", "world");
    mut->setFileMetadata("version", 1);

    int timeRange = 3600;

    Date start(2012, 01, 01, 00, 00, 00);

    //nSubjects = 5;
    //nBehaviors = 5;
    //nToRecord = 20;
    //timeRange = 5;

    mut->hasSubjectIds = true;
    
    std::map<BH, std::map<SH, Date> > earliest;

    for (unsigned i = 0;  i < nToRecord;  ++i) {
        Id subject(random() % nSubjects + 1);
        Id behavior(random() % nBehaviors + 1);
        uint32_t count(random() % 10 + 1);
        Date timestamp = start.plusSeconds(random() % timeRange);

        //cerr << "recording " << subject << " did " << behavior << " at "
        //     << timestamp << ", " << count << " times" << endl;

        mut->record(subject, behavior, timestamp, count);
        if (i % 1000 == 0) cerr << "\r" << (i / 1000) << "k keys";
        // if (i % 10 == 0) ExcAssert(!checkCounts(*mut));

        Date & e = earliest[BH(behavior.hash())][SH(subject.hash())];
        if (e == Date() || timestamp < e)
            e = timestamp;
    }
    cerr << endl;


    auto dumpMut MLDB_UNUSED = [&] ()
        {
            cerr << hex;

            auto onSubject = [&] (SH subject, const SubjectIterInfo &)
            {
                cerr << "sbj(" << subject << "): " << flush;

                cerr << mut->getSubjectId(subject) << endl;

                auto onBeh = [&] (BH beh, Date ts, uint32_t count)
                {
                    cerr << "\tbeh(" << beh << "): " << flush;
                    cerr << mut->getBehaviorId(beh) << " " << ts << " " << count << endl;
                    return true;
                };

                mut->forEachSubjectBehaviorHash(subject, onBeh);
                return true;
            };

            mut->forEachSubject(onSubject);

            cerr << dec;
        };

    //cerr << "before finish" << endl;
    //mut->dump(cerr);

    //dumpMut();

    mut->finish();

    //cerr << "after finish" << endl;
    //mut->dump(cerr);

    cerr << "added " << nBehaviors << " behaviors" << endl;

    vector<BH> allBehaviors = mut->allBehaviorHashes();

    for (unsigned i = 0;  i < allBehaviors.size();  ++i) {
        BH beh = allBehaviors[i];
        auto ts = mut->getSubjectHashesAndTimestamps(beh);
        for (auto it = ts.begin(); it != ts.end();  ++it) {

            if (earliest[beh][it->first] != it->second) {
                cerr << "beh = " << beh << " subj = " << it->first << endl;
            }
            BOOST_CHECK_EQUAL(earliest[beh][it->first], it->second);
        }
    }

    //dumpMut();
}

