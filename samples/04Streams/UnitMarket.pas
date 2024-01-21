unit UnitMarket;

interface

uses
  System.Generics.Collections,
  System.Threading, LoggerPro.GlobalLogger, Redis.Commons, Redis.Client, Redis.Values, Redis.NetLib.Indy;

type
  TSide = (Buy, Sell);

  TOrder = class
  public
    CreatorID: string;
    Side: TSide;
    Quantity: double;
    Price: double;

    constructor Create(id:string);  overload;
  end;

  TMatch = class
  public
    Bid: TOrder;
    Offer: TOrder;
  end;

  TMarket = class
  private
    FBids: TObjectList<TOrder>;
    FOffers: TObjectList<TOrder>;
    FMatches: TObjectList<TMatch>;
  public
    constructor Create;
    destructor Destroy; override;

    procedure AddOrder(order: TOrder);

    function ComputeClearingPrice: Integer;

    property Bids: TObjectList<TOrder> read FBids;
    property Offers: TObjectList<TOrder> read FOffers;
    property Matches: TObjectList<TMatch> read FMatches;
  end;

  const
   XMin = 0;
   XMax = 1000000000;

       procedure MatchOrders;

implementation

uses
  System.SysUtils, MainFormU, StrUtils;

{ TMarket }

constructor TMarket.Create;
begin
  FBids := TObjectList<TOrder>.Create;
  FOffers := TObjectList<TOrder>.Create;
  FMatches := TObjectList<TMatch>.Create;
end;

destructor TMarket.Destroy;
begin
  FBids.Free;
  FOffers.Free;
  FMatches.Free;
  inherited;
end;

procedure TMarket.AddOrder(order: TOrder);
begin
  if order.Side = TSide.Sell then
    FOffers.Add(order)
  else
    FBids.Add(order);
end;

procedure Log(s:string);
begin
  MainForm.Memo1.Lines.Add(s);
end;



function StringToOrder(const orderString: string; side:TSide): TOrder;
var
  orderFields: TArray<string>;
begin
  orderFields := orderString.Split([',']);

  var lRedis := NewRedisClient();
   var  lCmd := NewRedisCommand('XRANGE');
    lCmd.Add('order_history:XH2USD');
    lCmd.Add(orderString);    lCmd.Add(orderString);

   var lRes: TRedisRESPArray := lRedis.ExecuteAndGetRESPArray(lCmd);
    if Assigned(lRes) then begin
     Log(lRes.ToJSON());

    end;

  lRedis.Disconnect();


//    if Length(orderFields) = 4 then
//  begin
   var lSizeOfMyStreamArray := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Count;
    Result := TOrder.Create;
    Result.CreatorID := lRes.Items[0].ArrayValue.Items[0].Value;
    Result.Side := side;
    Result.Quantity := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[5].value.todouble;
//         var str := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[7].value;
    Result.Price := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[7].value.todouble; //ArrayValue.Items[1].ArrayValue[7].Value.ToDouble;
//  end
//  else
//    raise Exception.Create('Invalid order string format');
end;

procedure GetOrderDetailsFromStream(Order:TOrder);
begin
 if (Order.Quantity >0) and (order.Price >0) then exit;

 var lRedis := NewRedisClient();
 var  lCmd := NewRedisCommand('XRANGE').Add('order_history:XH2USD')
                 .Add(Order.CreatorID).Add(Order.CreatorID);

 var lRes: TRedisRESPArray := lRedis.ExecuteAndGetRESPArray(lCmd);
 if Assigned(lRes) then Log(lRes.ToJSON());

 lRedis.Disconnect();

 var lSizeOfMyStreamArray := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Count;
 Order.Quantity := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[5].value.todouble;
 Order.Price := lRes.Items[0].ArrayValue.Items[1].ArrayValue.Items[7].value.todouble; //ArrayValue.Items[1].ArrayValue[7].Value.ToDouble;
end;

function OrderToString(const order: TOrder): string;
begin
  Result := Format('%d,%d,%d,%d', [order.CreatorID, Ord(order.Side), order.Quantity, order.Price]);
end;

procedure MatchOrders;
var
  newBid, newOffer, LBid, LOffer: TOrder;
  bids, offers: TRedisArray;
  NoNextCheck : boolean;
const
  bidsKey = 'BIDS:XH2USD';
  offersKey = 'OFFERS:XH2USD';
begin

  var RedisClient := NewRedisClient();
  // Retrieve bids and offers from Redis sorted sets


//  var
//  lCmd: IRedisCommand;
// var lRes: TRedisNullable<string>;
//  // ZADD BIDS:XH2USD 40000 1705351179675-0
//  lCmd := NewRedisCommand('ZRANGE').Add(bidsKey).Add('10000000000').Add('0').Add('BYSCORE').Add('REV').Add('LIMIT').Add('0').Add('1');
//  lRes := RedisClient.ExecuteWithStringResult(lCmd);

  bids   := RedisClient.ZRANGE(bidsKey, XMax, XMin, True, 1);
  offers := RedisClient.ZRANGE(offersKey, XMin, XMax, false, 1);

  // Process bids and offers
  if (bids.HasValue) and (Offers.HasValue) then
  begin
   NoNextCheck := false;
    // Convert bid and offer strings to TOrder objects (you need to implement this)
    LBid := TOrder.Create(Bids.items[0].tostring);
    GetOrderDetailsFromStream(LBid);
    LOffer := TOrder.create(Offers.items[0].tostring);
    GetOrderDetailsFromStream(LOffer);

    newBid := nil;    newOffer := nil;

    if LBid.Price < LOffer.Price then begin
     LBid.Free;
     LOffer.Free;
     exit;
    end
    else
    begin
      if LBid.Quantity <> LOffer.Quantity then
      begin
        if LBid.Quantity > LOffer.Quantity then
        begin
          newBid := TOrder.Create;
          newBid.CreatorID := LBid.CreatorID;
          newBid.Quantity := LBid.Quantity - LOffer.Quantity;
          newBid.Price := LBid.Price;

          LBid.Quantity := LOffer.Quantity;     //Kupi³em ile bu³o do sprzedania
        end
        else
        begin
          newOffer := TOrder.Create;
          newOffer.CreatorID := LOffer.CreatorID;
          newOffer.Quantity := LOffer.Quantity - LBid.Quantity;
          newOffer.Price := LOffer.Price;

          LOffer.Quantity := LBid.Quantity;            // sprzeda³em ile by³o do kupienia
        end;
      end;
    end;

//  FMatches.Add(TMatch.Create);
//  FMatches.Last.Bid := LBid;
//  FMatches.Last.Offer := LOffer;
  redisclient.XADD('Matches',LBid.CreatorID,LOffer.CreatorID);
  Log('Match! '+LBid.CreatorID+' '+LOffer.CreatorID);

  RedisClient.ZREM(bidsKey, LBid.CreatorID);
  RedisClient.ZREM(offersKey, LOffer.CreatorID);
  RedisClient.DEL([bidsKey,offersKey]);
//  RedisClient.HDEL(offersKey,['type','qty','price']);

  bids.Empty;
  offers.Empty;

  if assigned(newBid) then
  NewOrderConsumer1(newBid.CreatorID,'BUY',newBid.Quantity.ToString,newBid.Price.ToString);
  if assigned(newOffer) then
  NewOrderConsumer1(newoffer.CreatorID,'SELL',newoffer.Quantity.ToString,newoffer.Price.ToString);

  LBid.Free;
  LOffer.Free;

//  if not NoNextCheck then MainForm.CheckMatches;
  end;

end;


function TMarket.ComputeClearingPrice: Integer;
var
  Match: TMatch;
  CumulativeQuantity, ClearingPrice: double;
begin
  if FMatches.Count = 0 then
    Exit(0);

  ClearingPrice := 0;
  CumulativeQuantity := 0;

  for Match in FMatches do
  begin
    CumulativeQuantity := CumulativeQuantity + Match.Bid.Quantity;
    ClearingPrice := ClearingPrice + Match.Bid.Quantity * ((Match.Bid.Price + Match.Offer.Price) / 2);
  end;

  Result := Round(ClearingPrice / CumulativeQuantity);
end;

{ TOrder }

constructor TOrder.Create(id: string);
begin
 CreatorID := id;
end;

end.

