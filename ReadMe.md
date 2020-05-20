```java
@Service
@Slf4j
public class ReportFinancialReconciliationService extends EntityService<ReportFinancialReconciliation> {

    @Autowired
    private ReportOrderTradeMapper reportOrderTradeMapper;
    @Autowired
    private ReportFinancialReconciliationMapper mapper;
    @Autowired
    TradeRecordMapper tradeRecordMapper;

    @Override
    public Mono<Page<ReportFinancialReconciliation>> queryByPage(Context ctx, Page<ReportFinancialReconciliation> queryPage) {
        Example example = Example.builder(ReportFinancialReconciliation.class).orderByDesc("reportTime").build();
        Example.Criteria criteria = createCriteria(ctx, example);
        SearchCriteriaUtils.getSearchCriteria(example, criteria, ReportFinancialReconciliation.class, queryPage.getFilter());

        Flux<ReportFinancialReconciliation> listData = Flux.defer(() -> just(mapper.selectByExampleAndRowBounds(example, queryPage.getRowBounds())));
        Mono<Integer> count = Mono.defer(() -> just(mapper.selectCountByExample(example)));
        return listData.collectList().zipWith(count, (list, c) -> {
            queryPage.setData(list);
            queryPage.setTotal(c);
            return queryPage;
        });
    }

    public Mono<List<ReportFinancialReconciliation>> queryByFilter(Context ctx,String fliter){
        Example example = Example.builder(ReportFinancialReconciliation.class).orderByDesc("reportTime").build();
        Example.Criteria criteria = createCriteria(ctx, example);
        SearchCriteriaUtils.getSearchCriteria(example, criteria, ReportFinancialReconciliation.class, fliter);
        Flux<ReportFinancialReconciliation> listData = Flux.defer(() -> just(mapper.selectByExample(example)));
        return listData.collectList();
    }

    public Mono<ReloadListVO> cacheTheOlderDate(){
        Context ctx = ContextWrapper.getContext();
        ReloadListVO reloadListVO = new ReloadListVO();
        Example example = Example.builder(TradeRecord.class).orderByAsc("createdDate").build();
        Example.Criteria criteria = createCriteria(ctx, example);
        List<TradeRecord> tradeRecordList = tradeRecordMapper.selectByExample(example);
        Date yesterday = tradeRecordList.get(0).getCreatedDate();
        yesterday = DateUtils.ZeroPointOfTheDay(yesterday);
        Date mxDay = DateUtils.addDays(yesterday, +1);
        Date today = DateUtils.ZeroPointOfTheDay(Date.from(Instant.now()));
        reloadListVO.setMinDay(yesterday);
        reloadListVO.setToday(today);
        List<DaysVo> daysVoList = new ArrayList<DaysVo>();
        while (!DateUtils.isSameDay(mxDay,DateUtils.addDays(today, +1))){
            DaysVo daysVo = new DaysVo();
            daysVo.setYesterday(yesterday);
            daysVo.setToday(mxDay);
            daysVoList.add(daysVo);
            yesterday = mxDay;
            mxDay = DateUtils.addDays(mxDay, +1);
        }
        Mono.just(daysVoList).flatMapMany(Flux::fromIterable).parallel().runOn(Schedulers.parallel())
                .doOnNext(vo->{cacheReportFinancialReconciliation(vo.getToday(),vo.getYesterday());
        }).subscribe();

        return just(reloadListVO);
    }

    public void cacheReportFinancialReconciliation(Date today, Date yesterday) {
        Context ctx = ContextWrapper.getContext();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String yesterdaySdf = sdf.format(yesterday);
        Date yesterdayDate = DateUtils.ZeroPointOfTheDay(yesterday);
        ReportFinancialReconciliation reportFinancialReconciliation = new ReportFinancialReconciliation();
        List<CacheTicketTradeVo> cacheTicketTradeVoList = reportOrderTradeMapper.findTicketTrade(yesterdayDate);
        log.info("today:{}--yesterday:{}--本日交易条数：{}", today, yesterdaySdf, cacheTicketTradeVoList.size());
        if(cacheTicketTradeVoList.size()>0){
            cacheTicketTradeVoList.forEach(value->{
                if (TradeType.EXPENSE.equals(value.getTradeType())) {
                    BigDecimal tradeAmount = getZeroIfNull(value.getExpenseAmount());
                    reportFinancialReconciliation.setExpenseAmount(getZeroIfNull(reportFinancialReconciliation.getExpenseAmount()).add(tradeAmount));
                } else if (TradeType.EARNING.equals(value.getTradeType())) {
                    BigDecimal tradeAmount = getZeroIfNull(value.getEarningTotal());
                    switch (value.getPayment()) {
                        case CASH:
                            reportFinancialReconciliation.setCashEarningAmount(getZeroIfNull(reportFinancialReconciliation.getCashEarningAmount()).add(tradeAmount));
                            break;
                        case WECHAT:
                            reportFinancialReconciliation.setWechatEarningAmount(getZeroIfNull(reportFinancialReconciliation.getWechatEarningAmount()).add(tradeAmount));
                            break;
                        case ALIPAY:
                            reportFinancialReconciliation.setAlipayEarningAmount(getZeroIfNull(reportFinancialReconciliation.getAlipayEarningAmount()).add(tradeAmount));
                            break;
                    }
                }
            });
            reportFinancialReconciliation.setTotalAmount(getZeroIfNull(reportFinancialReconciliation.getCashEarningAmount())
                    .add(getZeroIfNull(reportFinancialReconciliation.getWechatEarningAmount()))
                    .add(getZeroIfNull(reportFinancialReconciliation.getAlipayEarningAmount()))
                    .subtract(getZeroIfNull(reportFinancialReconciliation.getExpenseAmount())));
        }
        reportFinancialReconciliation.setReportTime(yesterdaySdf);
        Example example = Example.builder(ReportFinancialReconciliation.class).build();
        Example.Criteria criteria = createCriteria(ctx, example);
        criteria.andEqualTo("reportTime", reportFinancialReconciliation.getReportTime());
        ReportFinancialReconciliation financialReconciliation = mapper.selectOneByExample(example);
        reportFinancialReconciliation.setModifiedDate(today);
        if(Objects.nonNull(financialReconciliation)){
            mapper.updateByExampleSelective(reportFinancialReconciliation,example);
        }else {
            reportFinancialReconciliation.setCreatedDate(today);
            mapper.insert(reportFinancialReconciliation);
        }

    }

    public void cacheReportFinancialReconciliationNeW(LocalDateTime nowTime, LocalDateTime yesterdayTime) {
        Context ctx = ContextWrapper.getContext();
        DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String yesterdaySdf = sdf.format(yesterdayTime);
        LocalDateTime yesterdayDate = LocalDateTime.of(yesterdayTime.toLocalDate(), LocalTime.MIN);
        ReportFinancialReconciliation reportFinancialReconciliation = new ReportFinancialReconciliation();
        List<CacheTicketTradeVo> cacheTicketTradeVoList = reportOrderTradeMapper.findTicketTradeNew(yesterdayDate);
        log.info("today:{}--yesterday:{}--本日交易条数：{}", nowTime.toLocalDate(), yesterdayTime.toLocalDate(), cacheTicketTradeVoList.size());
        if(cacheTicketTradeVoList.size()>0){
            cacheTicketTradeVoList.forEach(value->{
                if (TradeType.EXPENSE.equals(value.getTradeType())) {
                    BigDecimal tradeAmount = getZeroIfNull(value.getExpenseAmount());
                    reportFinancialReconciliation.setExpenseAmount(getZeroIfNull(reportFinancialReconciliation.getExpenseAmount()).add(tradeAmount));
                } else if (TradeType.EARNING.equals(value.getTradeType())) {
                    BigDecimal tradeAmount = getZeroIfNull(value.getEarningTotal());
                    switch (value.getPayment()) {
                        case CASH:
                            reportFinancialReconciliation.setCashEarningAmount(getZeroIfNull(reportFinancialReconciliation.getCashEarningAmount()).add(tradeAmount));
                            break;
                        case WECHAT:
                            reportFinancialReconciliation.setWechatEarningAmount(getZeroIfNull(reportFinancialReconciliation.getWechatEarningAmount()).add(tradeAmount));
                            break;
                        case ALIPAY:
                            reportFinancialReconciliation.setAlipayEarningAmount(getZeroIfNull(reportFinancialReconciliation.getAlipayEarningAmount()).add(tradeAmount));
                            break;
                    }
                }
            });
            reportFinancialReconciliation.setTotalAmount(getZeroIfNull(reportFinancialReconciliation.getCashEarningAmount())
                    .add(getZeroIfNull(reportFinancialReconciliation.getWechatEarningAmount()))
                    .add(getZeroIfNull(reportFinancialReconciliation.getAlipayEarningAmount()))
                    .subtract(getZeroIfNull(reportFinancialReconciliation.getExpenseAmount())));
        }
        reportFinancialReconciliation.setReportTime(yesterdaySdf);
        Example example = Example.builder(ReportFinancialReconciliation.class).build();
        Example.Criteria criteria = createCriteria(ctx, example);
        criteria.andEqualTo("reportTime", reportFinancialReconciliation.getReportTime());
        ReportFinancialReconciliation financialReconciliation = mapper.selectOneByExample(example);
        Date today = DateUtils.localDateTimeToDate(nowTime);

        reportFinancialReconciliation.setModifiedDate(today);
        if(Objects.nonNull(financialReconciliation)){
            mapper.updateByExampleSelective(reportFinancialReconciliation,example);
        }else {
            reportFinancialReconciliation.setCreatedDate(today);
            mapper.insert(reportFinancialReconciliation);
        }
    }

    private BigDecimal getZeroIfNull(BigDecimal amount) {
        return Optional.ofNullable(amount).orElse(BigDecimal.ZERO);
    }

    @Override
    public Mapper<ReportFinancialReconciliation> getEntityMapper() {
        return mapper;
    }
}
```
